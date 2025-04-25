#!/bin/bash
set -ex

THIS_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
DOCKER_BUILD_DIR=$(realpath ${THIS_DIR}/../../docker/clickhouse-server)
DOCKER_BUILD_FILE="${DOCKER_BUILD_DIR}/from_cribl/Dockerfile"

if [ $# -ne 1 ]; then
  >&2 echo "usage: $0 <absolute path to ClickHouse binary>"
  exit 1
elif [ ! -e "${1}" ]; then
  >&2 echo "'${1}' does not exist!"
  exit 1
fi

# Copy the binary to the docker build dir
cp ${1} ${DOCKER_BUILD_DIR}/

REGION="us-west-2"
ECR_REG="${SAAS_OPS_ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
ECR_REPO="${ECR_REG}/cribl-cloud/clickhouse-server"
BASE_IMAGE_REPO="${SAAS_OPS_ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/cribl-cloud/ubuntu-base"
# TODO: Replace the commit with a semver once that's added
VERSION_TAG="$(git rev-parse HEAD)"

aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ECR_REG} || exit 1
docker build --build-arg IMAGE_REPO=${BASE_IMAGE_REPO} -t ${ECR_REPO}:${VERSION_TAG} --push -f ${DOCKER_BUILD_FILE} ${DOCKER_BUILD_DIR}