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

service clickhouse-server start && sleep 5

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

    for i in $(seq 1 $NUM_TRIES); do
        clickhouse-test --testname --shard --zookeeper --hung-check --print-time "$SKIP_LIST_OPT" "${ADDITIONAL_OPTIONS[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a test_output/test_result.txt
        if [ ${PIPESTATUS[0]} -ne "0" ]; then
            break;
        fi
    done
}

export -f run_tests

timeout $MAX_RUN_TIME bash -c run_tests ||:
