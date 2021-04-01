#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test1";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test2";
$CLICKHOUSE_CLIENT --query "CREATE TABLE test1 (x UInt64) ENGINE = Memory";


function thread1()
{
    while true; do
        seq 1 1000 | sed -r -e 's/.+/RENAME TABLE test1 TO test2; RENAME TABLE test2 TO test1;/' | $CLICKHOUSE_CLIENT -n
    done
}

function thread2()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM merge('$CLICKHOUSE_DATABASE', '^test[12]$')"
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test1";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test2";
