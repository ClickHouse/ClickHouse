#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/src', '1') PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst (p UInt64, k String) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/dst', '1') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"

function thread1()
{
    while true;
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE src MOVE PARTITION 1 TO TABLE dst;" --query_id=query1
    done
}

function thread2()
{
    while true;
    do
        $CLICKHOUSE_CLIENT --query="INSERT INTO src SELECT number % 2, toString(number) FROM system.numbers LIMIT 100000" --query_id=query2
    done
}

function thread3()
{
    while true;
    do
        $CLICKHOUSE_CLIENT --query="SELECT * FROM src" --query_id=query3 1> /dev/null
    done
}

function thread4()
{
    while true;
    do
        $CLICKHOUSE_CLIENT --query="SELECT * FROM dst" --query_id=query4 1> /dev/null
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;

TIMEOUT=30

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &

wait

echo "DROP TABLE src NO DELAY" | ${CLICKHOUSE_CLIENT}
echo "DROP TABLE dst NO DELAY" | ${CLICKHOUSE_CLIENT}
sleep 5

# Check for deadlocks
echo "SELECT * FROM system.processes WHERE query_id LIKE 'query%'" | ${CLICKHOUSE_CLIENT}

echo 'did not crash'
