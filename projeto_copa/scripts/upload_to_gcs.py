"""
Script de upload de imagens para o Google Cloud Storage
Pipeline de Engenharia de Dados - Copa do Mundo
"""

import os
from pathlib import Path
from google.cloud import storage

# =============================================================
# CONFIGURACOES — edite apenas esta secao
# =============================================================
# caminho para o seu arquivo .json de credenciais do GCP
CREDENTIALS_PATH = "credenciais.json" 
# nome do bucket que voce criou no Datalake para armazenar as imagens  
BUCKET_NAME      = "bucket-copa"
# pasta local onde estao as imagens que voce quer enviar para o GCS    
PASTA_IMAGENS    = "images" 
# prefixo (pasta) dentro do bucket onde as imagens serao armazenadas no GCS           
DESTINO_GCS      = "imagens_jogadores/"           


def conectar_bucket(credentials_path: str, bucket_name: str):
    """Autentica com o GCP e retorna o objeto bucket."""
    client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.bucket(bucket_name)
    print(f"Conectado ao bucket: {bucket_name}")
    return bucket


def listar_imagens(pasta: str) -> list[Path]:
    """Retorna uma lista com todos os arquivos de imagem da pasta."""
    extensoes_validas = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
    pasta_path = Path(pasta)

    if not pasta_path.exists():
        raise FileNotFoundError(f"Pasta nao encontrada: {pasta}")

    imagens = [
        arquivo
        for arquivo in pasta_path.iterdir()
        if arquivo.suffix.lower() in extensoes_validas
    ]

    print(f"{len(imagens)} imagem(ns) encontrada(s) em '{pasta}'")
    return imagens


def fazer_upload(bucket, imagens: list[Path], destino_gcs: str):
    """
    Envia cada imagem para o bucket.
    Pula arquivos que ja existem no GCS para evitar uploads duplicados.
    """
    enviados   = 0
    pulados    = 0
    com_erro   = 0

    for imagem in imagens:
        nome_destino = destino_gcs + imagem.name
        blob = bucket.blob(nome_destino)

        # Verifica se o arquivo ja existe no bucket
        if blob.exists():
            print(f"  [PULADO]  {imagem.name} ja existe no bucket")
            pulados += 1
            continue

        try:
            blob.upload_from_filename(str(imagem))
            print(f"  [OK]      {imagem.name}")
            enviados += 1
        except Exception as e:
            print(f"  [ERRO]    {imagem.name} — {e}")
            com_erro += 1

    return enviados, pulados, com_erro


def resumo(enviados: int, pulados: int, com_erro: int):
    """Exibe um resumo ao final do processo."""
    print("\n" + "=" * 40)
    print("Resumo do upload")
    print("=" * 40)
    print(f"  Enviados com sucesso : {enviados}")
    print(f"  Ja existiam (pulados): {pulados}")
    print(f"  Erros                : {com_erro}")
    print("=" * 40)


# =============================================================
# EXECUCAO
# =============================================================

if __name__ == "__main__":
    bucket  = conectar_bucket(CREDENTIALS_PATH, BUCKET_NAME)
    imagens = listar_imagens(PASTA_IMAGENS)
    enviados, pulados, com_erro = fazer_upload(bucket, imagens, DESTINO_GCS)
    resumo(enviados, pulados, com_erro)
