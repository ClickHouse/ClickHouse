#!/usr/bin/env bash
# Tags: no-fasttest, long

# Test that concurrent partition operations between the same pair of tables
# in opposite directions do not deadlock due to lock ordering.
# Covers MOVE PARTITION, ATTACH PARTITION FROM, and REPLACE PARTITION FROM.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t1"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t2"

$CLICKHOUSE_CLIENT --query="CREATE TABLE t1 (p UInt64, k String) ENGINE = MergeTree PARTITION BY p ORDER BY k"
$CLICKHOUSE_CLIENT --query="CREATE TABLE t2 (p UInt64, k String) ENGINE = MergeTree PARTITION BY p ORDER BY k"

function insert_thread()
{
    local table=$1
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="INSERT INTO $table SELECT number % 2, toString(number) FROM numbers(100)"
    done
}

function move_thread_forward()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE t1 MOVE PARTITION 1 TO TABLE t2"
    done
}

function move_thread_backward()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE t2 MOVE PARTITION 1 TO TABLE t1"
    done
}

function attach_thread_forward()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE t1 ATTACH PARTITION 1 FROM t2"
    done
}

function attach_thread_backward()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE t2 ATTACH PARTITION 1 FROM t1"
    done
}

function replace_thread_forward()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE t1 REPLACE PARTITION 1 FROM t2"
    done
}

function replace_thread_backward()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query="ALTER TABLE t2 REPLACE PARTITION 1 FROM t1"
    done
}

TIMEOUT=10

insert_thread t1 2>/dev/null &
insert_thread t2 2>/dev/null &
move_thread_forward 2>/dev/null &
move_thread_forward 2>/dev/null &
move_thread_backward 2>/dev/null &
move_thread_backward 2>/dev/null &
attach_thread_forward 2>/dev/null &
attach_thread_backward 2>/dev/null &
replace_thread_forward 2>/dev/null &
replace_thread_backward 2>/dev/null &

wait

$CLICKHOUSE_CLIENT --query="DROP TABLE t1"
$CLICKHOUSE_CLIENT --query="DROP TABLE t2"

echo 'ok'
