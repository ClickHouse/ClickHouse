#!/usr/bin/env bash
set -ex
BASE_DIR=$(dirname $(readlink -f $0))
BUILD_DIR="${BASE_DIR}/../build"
IMAGE="clickhouse/website"
if [[ -z "$1" ]]
then
    TAG=$(head -c 8 /dev/urandom | xxd -p)
else
    TAG="$1"
fi
FULL_NAME="${IMAGE}:${TAG}"
REMOTE_NAME="registry.yandex.net/${FULL_NAME}"
DOCKER_HASH="$2"
if [[ -z "$1" ]]
then
    source "${BASE_DIR}/venv/bin/activate"
    python "${BASE_DIR}/build.py" "--enable-stable-releases"
    cd "${BUILD_DIR}"
    docker build -t "${FULL_NAME}" "${BUILD_DIR}"
    docker tag "${FULL_NAME}" "${REMOTE_NAME}"
    DOCKER_HASH=$(docker push "${REMOTE_NAME}" | tail -1 | awk '{print $3;}')
    docker rmi "${FULL_NAME}"
fi

QLOUD_ENDPOINT="https://platform.yandex-team.ru/api/v1"
QLOUD_PROJECT="clickhouse.clickhouse-website"
if [[ -z "$1" ]]
then
    QLOUD_ENV="${QLOUD_PROJECT}.test"
else
    QLOUD_ENV="${QLOUD_PROJECT}.prod"
fi
QLOUD_COMPONENT="${QLOUD_ENV}.nginx"
QLOUD_VERSION=$(curl -v -H "Authorization: OAuth ${QLOUD_TOKEN}" "${QLOUD_ENDPOINT}/environment/status/${QLOUD_ENV}" | python -c "import json; import sys; print json.loads(sys.stdin.read()).get('version')")
curl -v -H "Authorization: OAuth ${QLOUD_TOKEN}" -H "Content-Type: application/json" --data "{\"repository\": \"${REMOTE_NAME}\", \"hash\": \"${DOCKER_HASH}\"}" "${QLOUD_ENDPOINT}/component/${QLOUD_COMPONENT}/${QLOUD_VERSION}/deploy" > /dev/null

echo ">>> Successfully deployed ${TAG} ${DOCKER_HASH} to ${QLOUD_ENV} <<<"
