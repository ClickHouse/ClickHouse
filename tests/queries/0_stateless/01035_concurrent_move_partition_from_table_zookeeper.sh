#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', '1') PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst (p UInt64, k String) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', '1') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"

function thread1()
{
    $CLICKHOUSE_CLIENT --query="ALTER TABLE src MOVE PARTITION 1 TO TABLE dst"
}

function thread2()
{
    $CLICKHOUSE_CLIENT --query="INSERT INTO src SELECT number % 2, toString(number) FROM system.numbers LIMIT 100000"
}

function thread3()
{
    $CLICKHOUSE_CLIENT --query="SELECT * FROM src" > /dev/null
}

function thread4()
{
    $CLICKHOUSE_CLIENT --query="SELECT * FROM dst" > /dev/null
}

function thread5()
{
    $CLICKHOUSE_CLIENT --query="ALTER TABLE src MOVE PARTITION 1 TO TABLE dst"
}

export -f thread1
export -f thread2
export -f thread3
export -f thread4
export -f thread5

TIMEOUT=30

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &

wait

echo "DROP TABLE src NO DELAY" | ${CLICKHOUSE_CLIENT}
echo "DROP TABLE dst NO DELAY" | ${CLICKHOUSE_CLIENT}

echo 'did not crash'
