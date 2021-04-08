#!/bin/bash

# fail on errors, verbose and export all env variables
set -e -x -a

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

# install test configs
/usr/share/clickhouse-test/config/install.sh

# For flaky check we also enable thread fuzzer
if [ "$NUM_TRIES" -gt "1" ]; then
    export THREAD_FUZZER_CPU_TIME_PERIOD_US=1000
    export THREAD_FUZZER_SLEEP_PROBABILITY=0.1
    export THREAD_FUZZER_SLEEP_TIME_US=100000

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY=1

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US=10000

    # simpliest way to forward env variables to server
    sudo -E -u clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --daemon
    sleep 5
else
    service clickhouse-server start && sleep 5
fi

if grep -q -- "--use-skip-list" /usr/bin/clickhouse-test; then
    SKIP_LIST_OPT="--use-skip-list"
fi

function run_tests()
{
    # We can have several additional options so we path them as array because it's
    # more idiologically correct.
    read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"

    # Skip these tests, because they fail when we rerun them multiple times
    if [ "$NUM_TRIES" -gt "1" ]; then
        ADDITIONAL_OPTIONS+=('--skip')
        ADDITIONAL_OPTIONS+=('00000_no_tests_to_skip')
    fi

    for _ in $(seq 1 "$NUM_TRIES"); do
        clickhouse-test --testname --shard --zookeeper --hung-check --print-time "$SKIP_LIST_OPT" "${ADDITIONAL_OPTIONS[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a test_output/test_result.txt
        if [ "${PIPESTATUS[0]}" -ne "0" ]; then
            break;
        fi
    done
}

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests ||:
