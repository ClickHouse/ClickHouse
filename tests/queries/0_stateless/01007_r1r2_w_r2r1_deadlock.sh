#!/usr/bin/env bash
# Tags: deadlock, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS a"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS b"

$CLICKHOUSE_CLIENT --query "CREATE TABLE a (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT --query "CREATE TABLE b (x UInt8) ENGINE = MergeTree ORDER BY tuple()"


function thread1()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
        seq 1 100 | awk '{ print "SELECT x FROM a WHERE x IN (SELECT toUInt8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
        seq 1 100 | awk '{ print "SELECT x FROM b WHERE x IN (SELECT toUInt8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
    done
}

function thread3()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "ALTER TABLE a MODIFY COLUMN x Nullable(UInt8)"
        $CLICKHOUSE_CLIENT --query "ALTER TABLE a MODIFY COLUMN x UInt8"
    done
}

function thread4()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "ALTER TABLE b MODIFY COLUMN x Nullable(UInt8)"
        $CLICKHOUSE_CLIENT --query "ALTER TABLE b MODIFY COLUMN x UInt8"
    done
}

TIMEOUT=10

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE a"
$CLICKHOUSE_CLIENT --query "DROP TABLE b"
