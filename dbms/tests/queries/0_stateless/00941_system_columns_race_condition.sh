#!/usr/bin/env bash

# Test fix for issue #5066

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alter_table (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16)) ENGINE = MergeTree ORDER BY a"

function thread1()
{
    while true; do $CLICKHOUSE_CLIENT --query "SELECT name FROM system.columns UNION ALL SELECT name FROM system.columns FORMAT Null"; done
}

function thread2()
{
    while true; do $CLICKHOUSE_CLIENT -n --query "ALTER TABLE alter_table ADD COLUMN h String; ALTER TABLE alter_table MODIFY COLUMN h UInt64; ALTER TABLE alter_table DROP COLUMN h;"; done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;

timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table"
