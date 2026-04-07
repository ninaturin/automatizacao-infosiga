# Script para download e preparação automática dos dados do InfoSiga SP
# Autor: Gerência de Estudos, Monitoramento de Indicadores de Gestão do Conhecimento
# Secretaria de Tecnologia, Inovação e Desenvolvimento Econômico
# Osasco/SP
# Data: Janeiro/2026
# =====================================================================

# ---------------------------------------------------------------------
# IMPORTS DA BIBLIOTECA PADRÃO
# ---------------------------------------------------------------------

import requests
import pandas as pd
import sys
import subprocess
import importlib
import zipfile
import os
from pathlib import Path
from datetime import datetime
import shutil
import time

# ---------------------------------------------------------------------
# 1) GARANTIA DE DEPENDÊNCIAS EXTERNAS
# ---------------------------------------------------------------------


def ensure_package(pkg_name):
    try:
        importlib.import_module(pkg_name)
    except ImportError:
        print(f"[INFO] Instalando pacote '{pkg_name}'...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", pkg_name])


# ---------------------------------------------------------------------
# 2) IMPORTS DAS DEPENDÊNCIAS EXTERNAS
# ---------------------------------------------------------------------
ensure_package("requests")
ensure_package("pandas")

# ---------------------------------------------------------------------
# 3) CONFIGURAÇÃO DA PASTA COMPARTILHADA (EDITAR SOMENTE AQUI SE NECESSÁRIO)
# ---------------------------------------------------------------------
# ⚠️ ESTA É A ÚNICA PARTE DO CÓDIGO QUE DEPENDE DO AMBIENTE
# ⚠️ TODO O RESTANTE DO SCRIPT NÃO DEVE SER ALTERADO

BASE_COMPARTILHADA = Path(  # ADICIONAR O CAMINHO DA PASTA AQUI!
)

PASTA_ORIGEM = BASE_COMPARTILHADA / "Bases_Dados_Originais"
PASTA_FINAL = BASE_COMPARTILHADA / "Bancos_Finais"

PASTA_ORIGEM.mkdir(parents=True, exist_ok=True)
PASTA_FINAL.mkdir(parents=True, exist_ok=True)

print(f"[INFO] Pasta compartilhada utilizada:\n{BASE_COMPARTILHADA}")
print(f"[INFO] Salvando arquivos finais em:\n{PASTA_FINAL}\n")

# ---------------------------------------------------------------------
# 4) CONFIGURAÇÃO DO DOWNLOAD
# ---------------------------------------------------------------------

INFOSIGA_URL = "https://infosiga.detran.sp.gov.br/rest/painel/download/file/dados_infosiga.zip"
CAMINHO_ZIP = PASTA_ORIGEM / "dados_infosiga.zip"

# ---------------------------------------------------------------------
# 5) COLUNAS A REMOVER – BASE DE SINISTROS
# ---------------------------------------------------------------------

COLUNAS_REMOVER_SINISTROS = [
    "tp_sinistro_atropelamento",
    "tp_sinistro_colisao_frontal",
    "tp_sinistro_colisao_traseira",
    "tp_sinistro_colisao_lateral",
    "tp_sinistro_colisao_transversal",
    "tp_sinistro_colisao_outros",
    "tp_sinistro_choque",
    "tp_sinistro_capotamento",
    "tp_sinistro_engavetamento",
    "tp_sinistro_tombamento",
    "tp_sinistro_outros",
    "tp_sinistro_nao_disponivel"
]

# ---------------------------------------------------------------------
# 6) BASES A PROCESSAR
# ---------------------------------------------------------------------


BASES = {
    "pessoas":   {"prefix": "pessoas",   "drop_columns": None},
    "sinistros": {"prefix": "sinistros", "drop_columns": COLUNAS_REMOVER_SINISTROS},
    "veiculos":  {"prefix": "veiculos",  "drop_columns": None},
}


# ---------------------------------------------------------------------
# 7) FUNÇÕES DE SUPORTE AO PIPELINE
# ---------------------------------------------------------------------

def download_zip(url, dest_path):
    if dest_path.exists():
        print("[INFO] Removendo ZIP antigo antes do download...")
        dest_path.unlink()

    print("[INFO] Baixando dados do Infosiga...")
    r = requests.get(url, timeout=120)
    r.raise_for_status()

    with open(dest_path, "wb") as f:
        f.write(r.content)
        f.flush()
        os.fsync(f.fileno())

    print("[OK] Download concluído.")
    print(f"[DEBUG] ZIP salvo em: {dest_path}")
    print(f"[DEBUG] Tamanho do ZIP: {dest_path.stat().st_size} bytes")

    time.sleep(2)


def extract_zip(zip_path, dest_folder):
    print("[INFO] Extraindo arquivos do ZIP...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_folder)
    print("[OK] Extração finalizada.")


def list_csvs_with_prefix(prefix, folder):
    return sorted(
        [p for p in folder.rglob("*.csv") if prefix.lower() in p.name.lower()],
        key=lambda p: p.name.lower()
    )


def try_read_csv(path: Path):
    encodings = ["utf-8", "latin1"]
    seps = [",", ";", "|"]
    last_error = None

    for enc in encodings:
        for sep in seps:
            try:
                return pd.read_csv(
                    path,
                    sep=sep,
                    encoding=enc,
                    engine="c",
                    low_memory=False
                )
            except Exception as e:
                last_error = e

    for enc in encodings:
        for sep in seps:
            try:
                return pd.read_csv(
                    path,
                    sep=sep,
                    encoding=enc,
                    engine="python"
                )
            except Exception as e:
                last_error = e

    raise RuntimeError(f"Falha ao ler {path.name}: {last_error}")


def process_and_save(file_path: Path, drop_columns=None):
    df = try_read_csv(file_path)

    if drop_columns:
        df = df.drop(columns=drop_columns, errors="ignore")
        print(f"[INFO] Colunas removidas em {file_path.name}")

    out_path = PASTA_FINAL / file_path.name

    if out_path.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = out_path.with_name(
            f"{out_path.stem}_backup_{stamp}{out_path.suffix}"
        )
        shutil.copy2(out_path, backup)
        print(f"[INFO] Backup criado: {backup.name}")

    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[OK] Arquivo salvo: {out_path.name} ({len(df)} linhas)\n")


# ---------------------------------------------------------------------
# 8) FLUXO PRINCIPAL
# ---------------------------------------------------------------------


def main():
    print("\n=== PIPELINE INFOSIGA | BANCOS FINAIS (PASTA COMPARTILHADA) ===\n")

    download_zip(INFOSIGA_URL, CAMINHO_ZIP)
    extract_zip(CAMINHO_ZIP, PASTA_ORIGEM)

    for nome, cfg in BASES.items():
        print(f"== PROCESSANDO BASE: {nome.upper()} ==")
        arquivos = list_csvs_with_prefix(cfg["prefix"], PASTA_ORIGEM)

        if not arquivos:
            print("[AVISO] Nenhum arquivo encontrado.\n")
            continue

        for arq in arquivos:
            try:
                process_and_save(arq, cfg["drop_columns"])
            except Exception as e:
                print(f"[ERRO] {arq.name}: {e}\n")

    print("=== FIM DO PROCESSAMENTO ===\n")

# ---------------------------------------------------------------------
# 9) EXECUÇÃO
# ---------------------------------------------------------------------


if __name__ == "__main__":
    main()
