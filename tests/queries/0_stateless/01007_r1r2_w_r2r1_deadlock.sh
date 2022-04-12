#!/usr/bin/env bash
# Tags: deadlock, long

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
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    seq 1 100 | awk '{ print "SELECT x FROM a WHERE x IN (SELECT toUInt8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
}

function thread2()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    seq 1 100 | awk '{ print "SELECT x FROM b WHERE x IN (SELECT toUInt8(count()) FROM system.tables);" }' | $CLICKHOUSE_CLIENT -n
}

function thread3()
{
    $CLICKHOUSE_CLIENT --query "ALTER TABLE a MODIFY COLUMN x Nullable(UInt8)"
    $CLICKHOUSE_CLIENT --query "ALTER TABLE a MODIFY COLUMN x UInt8"
}

function thread4()
{
    $CLICKHOUSE_CLIENT --query "ALTER TABLE b MODIFY COLUMN x Nullable(UInt8)"
    $CLICKHOUSE_CLIENT --query "ALTER TABLE b MODIFY COLUMN x UInt8"
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

$CLICKHOUSE_CLIENT --query "DROP TABLE a"
$CLICKHOUSE_CLIENT --query "DROP TABLE b"
