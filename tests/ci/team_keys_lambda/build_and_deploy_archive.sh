#!/usr/bin/env bash
set -xeo pipefail

WORKDIR=$(dirname "$0")
cd "$WORKDIR"

PY_EXEC=python3.9
LAMBDA_NAME=$(basename "$PWD")
LAMBDA_NAME=${LAMBDA_NAME//_/-}
VENV=lambda-venv
rm -rf "$VENV" lambda-package.zip
"$PY_EXEC" -m venv "$VENV"
#virtualenv "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install -r requirements.txt
PACKAGE=lambda-package
rm -rf "$PACKAGE" "$PACKAGE".zip
cp -r "$VENV/lib/$PY_EXEC/site-packages" "$PACKAGE"
cp app.py "$PACKAGE"
rm -r "$PACKAGE"/{pip,pip-*,setuptools,setuptools-*}
( cd "$PACKAGE" && zip -r ../"$PACKAGE".zip . )

aws lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file fileb://"$PACKAGE".zip
