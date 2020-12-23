#!/usr/bin/env bash
set -ex

BASE_DIR=$(dirname $(readlink -f $0))
BUILD_DIR="${BASE_DIR}/../build"
PUBLISH_DIR="${BASE_DIR}/../publish"
BASE_DOMAIN="${BASE_DOMAIN:-content.clickhouse.tech}"
GIT_TEST_URI="${GIT_TEST_URI:-git@github.com:ClickHouse/clickhouse-website-content.git}"
GIT_PROD_URI="git@github.com:ClickHouse/clickhouse-website-content.git"
EXTRA_BUILD_ARGS="${EXTRA_BUILD_ARGS:---minify --verbose}"

if [[ -z "$1" ]]
then
    source "${BASE_DIR}/venv/bin/activate"
    python3 "${BASE_DIR}/build.py" ${EXTRA_BUILD_ARGS}
    rm -rf "${PUBLISH_DIR}"
    mkdir "${PUBLISH_DIR}" && cd "${PUBLISH_DIR}"

    # Will make a repository with website content as the only commit.
    git init
    git remote add origin "${GIT_TEST_URI}"
    git config user.email "robot-clickhouse@yandex-team.ru"
    git config user.name "robot-clickhouse"

    # Add files.
    cp -R "${BUILD_DIR}"/* .
    echo -n "${BASE_DOMAIN}" > CNAME
    echo -n "" > README.md
    echo -n "" > ".nojekyll"
    cp "${BASE_DIR}/../../LICENSE" .
    git add *
    git add ".nojekyll"

    # Push to GitHub rewriting the existing contents.
    git commit --quiet -m "Add new release at $(date)"
    git push --force origin master

    if [[ ! -z "${CLOUDFLARE_TOKEN}" ]]
    then
        sleep 1m
        python3 "${BASE_DIR}/purge_cache_for_changed_files.py"
    fi
fi
