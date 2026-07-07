#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, long, no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', '1') PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst (p UInt64, k String) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', '1') PARTITION BY p ORDER BY k
SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;"

function thread1()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE src MOVE PARTITION 1 TO TABLE dst;" --query_id=query1
    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="INSERT INTO src SELECT number % 2, toString(number) FROM system.numbers LIMIT 100000" --query_id=query2
    done
}

function thread3()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="SELECT * FROM src" --query_id=query3 1> /dev/null
    done
}

function thread4()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="SELECT * FROM dst" --query_id=query4 1> /dev/null
    done
}

function thread5()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE src MOVE PARTITION 1 TO TABLE dst;" --query_id=query5
    done
}

TIMEOUT=20

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &

wait

echo "DROP TABLE src SYNC" | ${CLICKHOUSE_CLIENT}
echo "DROP TABLE dst SYNC" | ${CLICKHOUSE_CLIENT}
sleep 5

# Check for deadlocks
echo "SELECT * FROM system.processes WHERE query_id LIKE 'query%'" | ${CLICKHOUSE_CLIENT}

echo 'did not crash'
