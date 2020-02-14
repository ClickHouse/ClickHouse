#!/usr/bin/env bash
set -ex

BASE_DIR=$(dirname $(readlink -f $0))
BUILD_DIR="${BASE_DIR}/../build"
PUBLISH_DIR="${BASE_DIR}/../publish"
GIT_TEST_URI="git@github.com:ClickHouse/clickhouse-test.github.io.git"
GIT_PROD_URI="git@github.com:ClickHouse/clickhouse.github.io.git"

if [[ -z "$1" ]]
then
    TAG=$(head -c 8 /dev/urandom | xxd -p)
else
    TAG="$1"
fi
DOCKER_HASH="$2"
if [[ -z "$1" ]]
then
    source "${BASE_DIR}/venv/bin/activate"
    python "${BASE_DIR}/build.py" "--enable-stable-releases"
    rm -rf "${PUBLISH_DIR}" || true
    git clone "${GIT_TEST_URI}" "${PUBLISH_DIR}"
    cd "${PUBLISH_DIR}"
    git config user.email "robot-clickhouse@yandex-team.ru"
    git config user.name "robot-clickhouse"
    git rm -rf *
    cp -R "${BUILD_DIR}"/* .
    echo -n "test.clickhouse.tech" > CNAME
    echo -n "" > README.md
    echo -n "" > ".nojekyll"
    cp "${BASE_DIR}/../../LICENSE" .
    git add *
    git add ".nojekyll"
    git commit -a -m "add new release at $(date)"
    git push origin master
    cd "${BUILD_DIR}"
    DOCKER_HASH=$(head -c 16 < /dev/urandom | xxd -p)
else
    rm -rf "${BUILD_DIR}" || true
    rm -rf "${PUBLISH_DIR}" || true
    git clone "${GIT_TEST_URI}" "${BUILD_DIR}"
    git clone "${GIT_PROD_URI}" "${PUBLISH_DIR}"
    cd "${PUBLISH_DIR}"
    git config user.email "robot-clickhouse@yandex-team.ru"
    git config user.name "robot-clickhouse"
    git rm -rf *
    git commit -a -m "wipe old release"
    rm -rf "${BUILD_DIR}/.git"
    cp -R "${BUILD_DIR}"/* .
    echo -n "clickhouse.tech" > CNAME
    git add *
    git commit -a -m "add new release at $(date)"
    git push origin master
fi

QLOUD_ENDPOINT="https://platform.yandex-team.ru/api/v1"
QLOUD_PROJECT="clickhouse.clickhouse-website"
if [[ -z "$1" ]]
then
    QLOUD_ENV="${QLOUD_PROJECT}.test"
else
    QLOUD_ENV="${QLOUD_PROJECT}.prod"
fi
echo ">>> Successfully deployed ${TAG} ${DOCKER_HASH} to ${QLOUD_ENV} <<<"
