#!/usr/bin/env bash
set -xeo pipefail

WORKDIR=$(dirname "$0")
WORKDIR=$(readlink -f "${WORKDIR}")
DIR_NAME=$(basename "$WORKDIR")
cd "$WORKDIR"

# Do not deploy the lambda to AWS
DRY_RUN=${DRY_RUN:-}
# Python runtime to install dependencies
PY_VERSION=${PY_VERSION:-3.10}
PY_EXEC="python${PY_VERSION}"
# Image to build the lambda zip package
DOCKER_IMAGE="public.ecr.aws/lambda/python:${PY_VERSION}"
# Rename the_lambda_name directory to the-lambda-name lambda in AWS
LAMBDA_NAME=${DIR_NAME//_/-}
# The name of directory with lambda code
PACKAGE=lambda-package
rm -rf "$PACKAGE" "$PACKAGE".zip
mkdir "$PACKAGE"
cp app.py "$PACKAGE"
if [ -f requirements.txt ]; then
  VENV=lambda-venv
  rm -rf "$VENV" lambda-package.zip
  docker run --rm --user="${UID}" -e HOME=/tmp --entrypoint=/bin/bash \
    --volume="${WORKDIR}/..:/ci" --workdir="/ci/${DIR_NAME}" "${DOCKER_IMAGE}" \
    -exc "
      '$PY_EXEC' -m venv '$VENV' &&
      source '$VENV/bin/activate' &&
      pip install -r requirements.txt
    "
  cp -rT "$VENV/lib/$PY_EXEC/site-packages/" "$PACKAGE"
  rm -r "$PACKAGE"/{pip,pip-*,setuptools,setuptools-*}
fi
( cd "$PACKAGE" && zip -9 -r ../"$PACKAGE".zip . )

if [ -z "$DRY_RUN" ]; then
  aws lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file fileb://"$WORKDIR/$PACKAGE".zip
fi
