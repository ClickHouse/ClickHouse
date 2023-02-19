#!/usr/bin/env bash
set -xeo pipefail

WORKDIR=$(dirname "$0")
WORKDIR=$(readlink -f "${WORKDIR}")
cd "$WORKDIR"

PY_VERSION=3.9
PY_EXEC="python${PY_VERSION}"
DOCKER_IMAGE="python:${PY_VERSION}-slim"
LAMBDA_NAME=$(basename "$WORKDIR")
LAMBDA_NAME=${LAMBDA_NAME//_/-}
PACKAGE=lambda-package
rm -rf "$PACKAGE" "$PACKAGE".zip
mkdir "$PACKAGE"
cp app.py "$PACKAGE"
if [ -f requirements.txt ]; then
  VENV=lambda-venv
  rm -rf "$VENV" lambda-package.zip
  docker run --rm --user="${UID}" --volume="${WORKDIR}:/lambda" --workdir="/lambda" "${DOCKER_IMAGE}" \
    /bin/bash -c "
      '$PY_EXEC' -m venv '$VENV' &&
      source '$VENV/bin/activate' &&
      pip install -r requirements.txt
    "
  cp -rT "$VENV/lib/$PY_EXEC/site-packages/" "$PACKAGE"
  rm -r "$PACKAGE"/{pip,pip-*,setuptools,setuptools-*}
fi
( cd "$PACKAGE" && zip -9 -r ../"$PACKAGE".zip . )

aws lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file fileb://"$PACKAGE".zip
