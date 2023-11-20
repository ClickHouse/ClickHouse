#!/usr/bin/env bash
# Tags: deadlock, shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test1";
$CLICKHOUSE_CLIENT --query "CREATE TABLE test1 (x UInt8) ENGINE = MergeTree ORDER BY tuple()";

function thread1()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "ALTER TABLE test1 MODIFY COLUMN x Nullable(UInt8)"
        $CLICKHOUSE_CLIENT --query "ALTER TABLE test1 MODIFY COLUMN x UInt8"
    done
}

function thread2()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT x FROM test1 WHERE x IN (SELECT x FROM remote('127.0.0.2', '$CLICKHOUSE_DATABASE', test1))" --format Null
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE test1"
