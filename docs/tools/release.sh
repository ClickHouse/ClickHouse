#!/usr/bin/env bash
set -ex

BASE_DIR=$(dirname $(readlink -f $0))
BUILD_DIR="${BASE_DIR}/../build"
PUBLISH_DIR="${BASE_DIR}/../publish"
BASE_DOMAIN="${BASE_DOMAIN:-content.clickhouse.tech}"
GIT_TEST_URI="${GIT_TEST_URI:-git@github.com:ClickHouse/clickhouse-website-content.git}"
GIT_PROD_URI="git@github.com:ClickHouse/clickhouse-website-content.git"
EXTRA_BUILD_ARGS="${EXTRA_BUILD_ARGS:---enable-stable-releases --minify --verbose}"
HISTORY_SIZE="${HISTORY_SIZE:-5}"

if [[ -z "$1" ]]
then
    source "${BASE_DIR}/venv/bin/activate"
    python3 "${BASE_DIR}/build.py" ${EXTRA_BUILD_ARGS}
    rm -rf "${PUBLISH_DIR}" || true
    git clone "${GIT_TEST_URI}" "${PUBLISH_DIR}"
    cd "${PUBLISH_DIR}"
    git config user.email "robot-clickhouse@yandex-team.ru"
    git config user.name "robot-clickhouse"
    git rm -rf *
    cp -R "${BUILD_DIR}"/* .
    echo -n "${BASE_DOMAIN}" > CNAME
    echo -n "" > README.md
    echo -n "" > ".nojekyll"
    cp "${BASE_DIR}/../../LICENSE" .
    git add *
    git add ".nojekyll"
    git commit -a -m "add new release at $(date)"
    NEW_ROOT_COMMIT=$(git rev-parse "HEAD~${HISTORY_SIZE}")
    git checkout --orphan temp "${NEW_ROOT_COMMIT}"
    git commit -m "root commit"
    git rebase --onto temp "${NEW_ROOT_COMMIT}" master
    git branch -D temp
    git push -f origin master
    if [[ ! -z "${CLOUDFLARE_TOKEN}" ]]
    then
        sleep 1m
        python3 "${BASE_DIR}/purge_cache_for_changed_files.py"
    fi
fi
