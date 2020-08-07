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
    TAG=$(head -c 8 /dev/urandom | xxd -p)
else
    TAG="$1"
fi
DOCKER_HASH="$2"
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
        git diff --stat="9999,9999" --diff-filter=M HEAD~1 | grep '|' | awk '$1 ~ /\.html$/ { if ($3>8) { url="https://content.clickhouse.tech/"$1; sub(/\/index.html/, "/", url); print "\""url"\""; }}' | split -l 25 /dev/stdin PURGE
        for FILENAME in $(ls PURGE*)
        do
            POST_DATA=$(cat "${FILENAME}" | sed -n -e 'H;${x;s/\n/,/g;s/^,//;p;}' | awk '{print "{\"files\":["$0"]}";}')
            sleep 3s
            set +x
            curl -X POST "https://api.cloudflare.com/client/v4/zones/4fc6fb1d46e87851605aa7fa69ca6fe0/purge_cache" -H "Authorization: Bearer ${CLOUDFLARE_TOKEN}" -H "Content-Type:application/json" --data "${POST_DATA}"
            set -x
            rm "${FILENAME}"
        done
    fi
    cd "${BUILD_DIR}"
    DOCKER_HASH=$(head -c 16 < /dev/urandom | xxd -p)
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
