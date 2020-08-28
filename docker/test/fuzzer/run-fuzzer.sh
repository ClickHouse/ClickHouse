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
    ln -s ./clickhouse ./clickhouse-server
    ln -s ./clickhouse ./clickhouse-client
}

function configure
{
    rm -rf db ||:
    mkdir db ||:
    cp -av "$repo_dir"/programs/server/config* db
    cp -av "$repo_dir"/programs/server/user* db
    # TODO figure out which ones are needed
    cp -av "$repo_dir"/tests/config/listen.xml db/config.d
    cp -av "$script_dir"/query-fuzzer-tweaks-users.xml db/users.d
}

function watchdog
{
    sleep 3600

    echo "Fuzzing run has timed out"
    killall clickhouse-client ||:
    for x in {1..10}
    do
        if ! pgrep -f clickhouse-client
        then
            break
        fi
        sleep 1
    done

    killall -9 clickhouse-client ||:
}

function fuzz
{
    ./clickhouse-server --config-file db/config.xml -- --path db 2>&1 | tail -10000 > server.log &
    server_pid=$!
    kill -0 $server_pid
    while ! ./clickhouse-client --query "select 1" && kill -0 $server_pid ; do echo . ; sleep 1 ; done
    ./clickhouse-client --query "select 1"
    kill -0 $server_pid
    echo Server started

    fuzzer_exit_code=0
    ./clickhouse-client --query-fuzzer-runs=1000 \
        < <(for f in $(ls ch/tests/queries/0_stateless/*.sql | sort -R); do cat "$f"; echo ';'; done) \
        > >(tail -10000 > fuzzer.log) \
        2>&1 \
        || fuzzer_exit_code=$?
    
    echo "Fuzzer exit code is $fuzzer_exit_code"

    ./clickhouse-client --query "select elapsed, query from system.processes" ||:
    killall clickhouse-server ||:
    for x in {1..10}
    do
        if ! pgrep -f clickhouse-server
        then
            break
        fi
        sleep 1
    done
    killall -9 clickhouse-server ||:
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
        time ch/docker/test/fuzzer/run-fuzzer.sh
        # Keep the error code
        exit $?
    fi
    ;&
"download")
    time download
    ;&
"configure")
    time configure
    ;&
"fuzz")
    # Start a watchdog that should kill the fuzzer on timeout.
    # The shell won't kill the child sleep when we kill it, so we have to put it
    # into a separate process group so that we can kill them all.
    set -m
    watchdog &
    watchdog_pid=$!
    set +m
    # Check that the watchdog has started
    kill -0 $watchdog_pid

    fuzzer_exit_code=0
    time fuzz || fuzzer_exit_code=$?
    kill -- -$watchdog_pid ||:

    # Debug
    date
    sleep 10
    jobs
    pstree -aspgT

    # Make files with status and description we'll show for this check on Github
    task_exit_code=$fuzzer_exit_code
    if [ "$fuzzer_exit_code" == 143 ]
    then
        # SIGTERM -- the fuzzer was killed by timeout, which means a normal run.
        echo "success" > status.txt
        echo "OK" > description.txt
        task_exit_code=0
    elif [ "$fuzzer_exit_code" == 210 ]
    then
        # Lost connection to the server. This probably means that the server died
        # with abort.
        echo "failure" > status.txt
        if ! grep -ao "Received signal.*\|Logical error.*\|Assertion.*failed" server.log > description.txt
        then
            echo "Lost connection to server. See the logs" > description.txt
        fi
    else
        # Something different -- maybe the fuzzer itself died? Don't grep the
        # server log in this case, because we will find a message about normal
        # server termination (Received signal 15), which is confusing.
        echo "failure" > status.txt
        echo "Fuzzer failed ($fuzzer_exit_code). See the logs" > description.txt
    fi

    exit $task_exit_code
    ;&
esac

