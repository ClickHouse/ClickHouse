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

# Do not rebuild and deploy the archive if it's newer than sources
if [ -e "$PACKAGE.zip" ] && [ -z "$FORCE" ]; then
  REBUILD=""
  for src in app.py build_and_deploy_archive.sh requirements.txt lambda_shared/*; do
    if [ "$src" -nt "$PACKAGE.zip" ]; then
      REBUILD=1
    fi
  done
  [ -n "$REBUILD" ] || exit 0
fi

rm -rf "$PACKAGE" "$PACKAGE".zip
mkdir "$PACKAGE"
cp app.py "$PACKAGE"
if [ -f requirements.txt ]; then
  VENV=lambda-venv
  rm -rf "$VENV"
  docker run --net=host --rm --user="${UID}" -e HOME=/tmp --entrypoint=/bin/bash \
    --volume="${WORKDIR}/..:/ci" --workdir="/ci/${DIR_NAME}" "${DOCKER_IMAGE}" \
    -exc "
      '$PY_EXEC' -m venv '$VENV' &&
      source '$VENV/bin/activate' &&
      pip install -r requirements.txt &&
      # To have consistent pyc files
      find '$VENV/lib' -name '*.pyc' -delete
      find '$VENV/lib' ! -type d -exec touch -t 201212121212 {} +
      python -m compileall
    "
  cp -rT "$VENV/lib/$PY_EXEC/site-packages/" "$PACKAGE"
  rm -r "$PACKAGE"/{pip,pip-*,setuptools,setuptools-*}
  # zip stores metadata about timestamps
  find "$PACKAGE" ! -type d -exec touch -t 201212121212 {} +
fi
(
  export LC_ALL=c
  cd "$PACKAGE"
  # zip uses random files order by default, so we sort the files alphabetically
  find . ! -type d -print0 | sort -z | tr '\0' '\n' | zip -XD -0 ../"$PACKAGE".zip --names-stdin
)

ECHO=()
if [ -n "$DRY_RUN" ]; then
  ECHO=(echo Run the following command to push the changes:)
fi
"${ECHO[@]}" aws lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file fileb://"$WORKDIR/$PACKAGE".zip
