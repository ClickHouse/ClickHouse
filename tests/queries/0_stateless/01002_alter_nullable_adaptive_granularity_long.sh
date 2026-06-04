#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test";
$CLICKHOUSE_CLIENT --query "CREATE TABLE test (x UInt8, s String MATERIALIZED toString(rand64())) ENGINE = MergeTree ORDER BY s";

function thread1()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "INSERT INTO test SELECT rand() FROM numbers(1000)";
    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -n --query "ALTER TABLE test MODIFY COLUMN x Nullable(UInt8);";
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT -n --query "ALTER TABLE test MODIFY COLUMN x UInt8;";
        sleep 0.0$RANDOM
    done
}

function thread3()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -n --query "SELECT count() FROM test FORMAT Null";
    done
}

function thread4()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -n --query "OPTIMIZE TABLE test FINAL";
        sleep 0.1$RANDOM
    done
}

TIMEOUT=10

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE test"
