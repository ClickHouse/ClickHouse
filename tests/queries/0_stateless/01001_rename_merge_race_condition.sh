#!/usr/bin/env bash
# Tags: race, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test1"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test2"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test1 (x UInt64) ENGINE = Memory"


function thread1()
{
    seq 1 1000 | {
        sed -r -e 's/.+/RENAME TABLE test1 TO test2; RENAME TABLE test2 TO test1;/'
    } | $CLICKHOUSE_CLIENT -n
}

function thread2()
{
    $CLICKHOUSE_CLIENT --query "SELECT * FROM merge('$CLICKHOUSE_DATABASE', '^test[12]$')"
}

export -f thread1
export -f thread2

TIMEOUT=10

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test1"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test2"
