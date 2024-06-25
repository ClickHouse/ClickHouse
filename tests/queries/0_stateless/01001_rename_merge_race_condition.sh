#!/usr/bin/env bash
# Tags: race

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


TIMEOUT=10

spawn_with_timeout $TIMEOUT thread1 2> /dev/null
spawn_with_timeout $TIMEOUT thread2 2> /dev/null

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test1";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test2";
