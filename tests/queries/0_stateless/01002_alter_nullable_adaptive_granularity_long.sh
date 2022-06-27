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
    $CLICKHOUSE_CLIENT --query "INSERT INTO test SELECT rand() FROM numbers(1000)"
}

function thread2()
{
    $CLICKHOUSE_CLIENT -n --query "ALTER TABLE test MODIFY COLUMN x Nullable(UInt8);"
    sleep 0.0$RANDOM
    $CLICKHOUSE_CLIENT -n --query "ALTER TABLE test MODIFY COLUMN x UInt8;"
    sleep 0.0$RANDOM
}

function thread3()
{
    $CLICKHOUSE_CLIENT -n --query "SELECT count() FROM test FORMAT Null"
}

function thread4()
{
    $CLICKHOUSE_CLIENT -n --query "OPTIMIZE TABLE test FINAL"
    sleep 0.1$RANDOM
}

export -f thread1
export -f thread2
export -f thread3
export -f thread4

TIMEOUT=10

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE test"
