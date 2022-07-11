#!/usr/bin/env bash
set -ex

BASE_DIR=$(dirname "$(readlink -f "$0")")
BUILD_DIR="${BASE_DIR}/../build"
PUBLISH_DIR="${BASE_DIR}/../publish"
BASE_DOMAIN="${BASE_DOMAIN:-content.clickhouse.com}"
GIT_PROD_URI="${GIT_PROD_URI:-git@github.com:ClickHouse/clickhouse-com-content.git}"
EXTRA_BUILD_ARGS="${EXTRA_BUILD_ARGS:---verbose}"

if [[ -z "$1" ]]
then
    source "${BASE_DIR}/venv/bin/activate"
    # shellcheck disable=2086
    python3 "${BASE_DIR}/build.py" ${EXTRA_BUILD_ARGS}
    rm -rf "${PUBLISH_DIR}"
    mkdir "${PUBLISH_DIR}" && cd "${PUBLISH_DIR}"

    # Will make a repository with website content as the only commit.
    git init
    git remote add origin "${GIT_PROD_URI}"
    git config user.email "robot-clickhouse@clickhouse.com"
    git config user.name "robot-clickhouse"

    # Add files.
    cp -R "${BUILD_DIR}"/* .
    echo -n "${BASE_DOMAIN}" > CNAME
    echo -n "" > README.md
    echo -n "" > ".nojekyll"
    cp "${BASE_DIR}/../../LICENSE" .
    git add ./*
    git add ".nojekyll"

    git commit --quiet -m "Add new release at $(date)"

    # Push to GitHub rewriting the existing contents.
    # Sometimes it does not work with error message "! [remote rejected] master -> master (cannot lock ref 'refs/heads/master': is at 42a0f6b6b6c7be56a469441b4bf29685c1cebac3 but expected 520e9b02c0d4678a2a5f41d2f561e6532fb98cc1)"
    for _ in {1..10}; do git push --force origin master && break; sleep 5; done

    # Turn off logging.
    set +x

    if [[ -n "${CLOUDFLARE_TOKEN}" ]]
    then
        sleep 1m
        # https://api.cloudflare.com/#zone-purge-files-by-cache-tags,-host-or-prefix
        POST_DATA='{"hosts":["clickhouse.com"]}'
        curl -X POST "https://api.cloudflare.com/client/v4/zones/4fc6fb1d46e87851605aa7fa69ca6fe0/purge_cache" -H "Authorization: Bearer ${CLOUDFLARE_TOKEN}" -H "Content-Type:application/json" --data "${POST_DATA}"
    fi
fi
