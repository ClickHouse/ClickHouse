#!/usr/bin/env bash
set -xeo pipefail

WORKDIR=$(dirname "$0")
cd "$WORKDIR"

PY_EXEC=python3.9
LAMBDA_NAME=$(basename "$PWD")
LAMBDA_NAME=${LAMBDA_NAME//_/-}
PACKAGE=lambda-package
rm -rf "$PACKAGE" "$PACKAGE".zip
mkdir "$PACKAGE"
cp app.py "$PACKAGE"
if [ -f requirements.txt ]; then
  VENV=lambda-venv
  rm -rf "$VENV" lambda-package.zip
  "$PY_EXEC" -m venv "$VENV"
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
  pip install -r requirements.txt
  cp -rT "$VENV/lib/$PY_EXEC/site-packages/" "$PACKAGE"
  rm -r "$PACKAGE"/{pip,pip-*,setuptools,setuptools-*}
fi
( cd "$PACKAGE" && zip -9 -r ../"$PACKAGE".zip . )

aws lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file fileb://"$PACKAGE".zip
