#!/usr/bin/env bash

# NOTE: $SECONDS accuracy is second, so we need some delta, hence -1 in time conditions.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function drop_tables()
{
    ${CLICKHOUSE_CLIENT} -nq "
        drop table if exists dist_01675;
        drop table if exists data_01675;
    "
}

#
# Case 1: max_delay_to_insert will throw.
#
function test_max_delay_to_insert_will_throw()
{
    echo "max_delay_to_insert will throw"

    local max_delay_to_insert=2
    ${CLICKHOUSE_CLIENT} -nq "
        create table data_01675 (key Int) engine=Null();
        create table dist_01675 (key Int) engine=Distributed(test_shard_localhost, currentDatabase(), data_01675) settings bytes_to_delay_insert=1, max_delay_to_insert=$max_delay_to_insert;
        system stop distributed sends dist_01675;
    "

    local start_seconds=$SECONDS
    ${CLICKHOUSE_CLIENT} --testmode -nq "
        -- first batch is always OK, since there is no pending bytes yet
        insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0;
        -- second will fail, because of bytes_to_delay_insert=1 and max_delay_to_insert>0,
        -- while distributed sends is stopped.
        --
        -- (previous block definitelly takes more, since it has header)
        insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0; -- { serverError 574 }
        system flush distributed dist_01675;
    "
    local end_seconds=$SECONDS

    if (( (end_seconds-start_seconds)<(max_delay_to_insert-1) )); then
        echo "max_delay_to_insert was not satisfied ($end_seconds-$start_seconds)"
    fi
}

#
# Case 2: max_delay_to_insert will finally finished.
#
function test_max_delay_to_insert_will_succeed_once()
{
    local max_delay_to_insert=4
    local flush_delay=2

    drop_tables

    ${CLICKHOUSE_CLIENT} -nq "
        create table data_01675 (key Int) engine=Null();
        create table dist_01675 (key Int) engine=Distributed(test_shard_localhost, currentDatabase(), data_01675) settings bytes_to_delay_insert=1, max_delay_to_insert=$max_delay_to_insert;
        system stop distributed sends dist_01675;
    "

    function flush_distributed_worker()
    {
        sleep $flush_delay
        ${CLICKHOUSE_CLIENT} -q "system flush distributed dist_01675"
    }
    flush_distributed_worker &

    local start_seconds=$SECONDS
    # ignore stderr, since it may produce exception if flushing thread will be too slow
    # (this is possible on CI)
    ${CLICKHOUSE_CLIENT} --testmode -nq "
        -- first batch is always OK, since there is no pending bytes yet
        insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0;
        -- second will succeed, due to SYSTEM FLUSH DISTRIBUTED in background.
        insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0;
    " >& /dev/null
    local end_seconds=$SECONDS
    wait

    local diff=$(( end_seconds-start_seconds ))

    if (( diff<(flush_delay-1) )); then
        # this is fatal error, that should not be retriable
        echo "max_delay_to_insert was not wait flush_delay ($diff)"
        exit 1
    fi

    # retry the test until the diff will be satisfied
    # (since we cannot assume that there will be no other lags)
    if (( diff>=(max_delay_to_insert-1) )); then
        return 1
    fi

    return 0
}
function test_max_delay_to_insert_will_succeed()
{
    echo "max_delay_to_insert will succeed"

    local retries=20 i=0
    while (( (i++) < retries )); do
        if test_max_delay_to_insert_will_succeed_once; then
            return
        fi
    done

    echo failed
}

function run_test()
{
    local test_case=$1 && shift

    drop_tables
    $test_case
}

function main()
{
    run_test test_max_delay_to_insert_will_throw
    run_test test_max_delay_to_insert_will_succeed

    drop_tables
}
main "$@"
