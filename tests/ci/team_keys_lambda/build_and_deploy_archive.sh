#!/usr/bin/env bash
set -xeo pipefail

VENV=lambda-venv
py_exec=$(which python3)
py_version=$(basename "$(readlink -f "$py_exec")")
rm -rf "$VENV" lambda-package.zip
virtualenv "$VENV"
source "$VENV/bin/activate"
pip install -r requirements.txt
PACKAGES="$VENV/lib/$py_version/site-packages"
cp app.py "$PACKAGES/"
( cd "$PACKAGES" && zip -r ../../../../lambda-package.zip . )

aws lambda update-function-code --function-name team-keys-lambda --zip-file fileb://lambda-package.zip
