#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS a"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS b"

$CLICKHOUSE_CLIENT --query "CREATE TABLE a (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query "CREATE TABLE b (x UInt8) ENGINE = MergeTree ORDER BY tuple()"


function thread1()
{
    while true; do 
        seq 1 100 | awk '{ print "SELECT x FROM a WHERE x IN (SELECT toUInt8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
    done
}

function thread2()
{
    while true; do 
        seq 1 100 | awk '{ print "SELECT x FROM b WHERE x IN (SELECT toUInt8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
    done
}

function thread3()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "ALTER TABLE a MODIFY COLUMN x Nullable(UInt8)"
        $CLICKHOUSE_CLIENT --query "ALTER TABLE a MODIFY COLUMN x UInt8"
    done
}

function thread4()
{
    while true; do 
        $CLICKHOUSE_CLIENT --query "ALTER TABLE b MODIFY COLUMN x Nullable(UInt8)"
        $CLICKHOUSE_CLIENT --query "ALTER TABLE b MODIFY COLUMN x UInt8"
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

$CLICKHOUSE_CLIENT --query "DROP TABLE a"
$CLICKHOUSE_CLIENT --query "DROP TABLE b"
