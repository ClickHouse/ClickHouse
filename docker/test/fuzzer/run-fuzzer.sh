#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

stage=${stage:-}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Doesn't work for clone stage, but should work after that
repo_dir=${repo_dir:-$(readlink -f "$script_dir/../../..")}

function clone
{
(
    rm -rf ch ||:
    mkdir ch
    cd ch

    git init
    git remote add origin https://github.com/ClickHouse/ClickHouse
    git fetch --depth=1 origin "$SHA_TO_TEST"

    # If not master, try to fetch pull/.../{head,merge}
    if [ "$PR_TO_TEST" != "0" ]
    then
        git fetch --depth=1 origin "refs/pull/$PR_TO_TEST/*:refs/heads/pull/$PR_TO_TEST/*"
    fi

    git checkout "$SHA_TO_TEST"
)
}

function download
{
#    wget -O- -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$PR_TO_TEST/$SHA_TO_TEST/clickhouse_build_check/performance/performance.tgz" \
#        | tar --strip-components=1 -zxv

    wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$PR_TO_TEST/$SHA_TO_TEST/clickhouse_build_check/clang-10_debug_none_bundled_unsplitted_disable_False_binary/clickhouse"
}

function configure
{
    mkdir db ||:
    cp -av "$repo_dir"/programs/server/config* db
    cp -av "$repo_dir"/programs/server/user* db
    cp -av "$repo_dir"/tests/config db/config.d
}

function fuzz
{
    ./clickhouse server --config-file db/config.xml -- --path db 2>&1 | tail -1000000 > server-log.txt &
    server_pid=$!
    kill -0 $server_pid
    while ! ./clickhouse client --query "select 1" && kill -0 $server_pid ; do echo . ; sleep 1 ; done
    ./clickhouse client --query "select 1"
    echo Server started

    for f in $(ls ch/tests/queries/0_stateless/*.sql | sort -R); do cat $f; echo ';'; done \
        | ./clickhouse client --query-fuzzer-runs=10 2>&1 | tail -1000000 > fuzzer-log.txt
}

case "$stage" in
"")
    ;&
"clone")
    time clone
    stage=download time ch/docker/test/fuzzer/run-fuzzer.sh
    ;;
"download")
    time download
    ;&
"configure")
    time configure
    ;&
"fuzz")
    time fuzz
    ;&
"report")
    ;&
esac

