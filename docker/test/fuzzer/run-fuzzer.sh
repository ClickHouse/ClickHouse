#!/bin/bash
set -eux
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

stage=${stage:-}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "$script_dir"
repo_dir=ch

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
    chmod +x clickhouse
}

function configure
{
    rm -rf db ||:
    mkdir db ||:
    cp -av "$repo_dir"/programs/server/config* db
    cp -av "$repo_dir"/programs/server/user* db
    cp -av "$repo_dir"/tests/config db/config.d
}

function watchdog
{
    sleep 3600

    echo "Fuzzing run has timed out"
    killall -9 clickhouse clickhouse-server clickhouse-client ||:
}

function fuzz
{
    ./clickhouse server --config-file db/config.xml -- --path db 2>&1 | tail -100000 > server.log &
    server_pid=$!
    kill -0 $server_pid
    while ! ./clickhouse client --query "select 1" && kill -0 $server_pid ; do echo . ; sleep 1 ; done
    ./clickhouse client --query "select 1"
    kill -0 $server_pid
    echo Server started

    fuzzer_exit_code=0
    ./clickhouse client --query-fuzzer-runs=1000 \
        < <(for f in $(ls ch/tests/queries/0_stateless/*.sql | sort -R); do cat "$f"; echo ';'; done) \
        > >(tail -100000 > fuzzer.log) \
        2>&1 \
        || fuzzer_exit_code=$?
    
    echo "Fuzzer exit code is $fuzzer_exit_code"
    kill -9 $server_pid ||:
    return $fuzzer_exit_code
}

case "$stage" in
"")
    ;&
"clone")
    time clone
    if [ -v FUZZ_LOCAL_SCRIPT ]
    then
        # just fall through
        echo Using the testing script from docker container
        :
    else
        # Run the testing script from the repository
        echo Using the testing script from the repository
        export stage=download
        # Keep the error code
        time ch/docker/test/fuzzer/run-fuzzer.sh || exit $?
    fi
    ;&
"download")
    time download
    ;&
"configure")
    time configure
    ;&
"fuzz")
    watchdog &
    watchdog_pid=$!
    fuzzer_exit_code=0
    time fuzz || fuzzer_exit_code=$?
    kill $watchdog_pid ||:

    # Debug
    date
    sleep 10
    jobs
    pstree -aspgT

    exit $fuzzer_exit_code
    ;&
esac

