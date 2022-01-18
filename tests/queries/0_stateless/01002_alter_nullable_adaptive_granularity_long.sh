#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test";
$CLICKHOUSE_CLIENT --query "CREATE TABLE test (x UInt8, s String MATERIALIZED toString(rand64())) ENGINE = MergeTree ORDER BY s";

function thread1()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "INSERT INTO test SELECT rand() FROM numbers(1000)";
    done
}

function thread2()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "ALTER TABLE test MODIFY COLUMN x Nullable(UInt8);";
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT -n --query "ALTER TABLE test MODIFY COLUMN x UInt8;";
        sleep 0.0$RANDOM
    done
}

function thread3()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "SELECT count() FROM test FORMAT Null";
    done
}

function thread4()
{
    while true; do
        $CLICKHOUSE_CLIENT -n --query "OPTIMIZE TABLE test FINAL";
        sleep 0.1$RANDOM
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE test"
