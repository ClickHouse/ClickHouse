#!/usr/bin/env bash
# Tags: race

# Test fix for issue #5066

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE IF EXISTS alter_table;
    CREATE TABLE alter_table (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16)) ENGINE = MergeTree ORDER BY a;
"

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    $CLICKHOUSE_CLIENT --query "SELECT name FROM system.columns UNION ALL SELECT name FROM system.columns FORMAT Null"
}

function thread2()
{
    $CLICKHOUSE_CLIENT -n --query "
        ALTER TABLE alter_table ADD COLUMN h String;
        ALTER TABLE alter_table MODIFY COLUMN h UInt64;
        ALTER TABLE alter_table DROP COLUMN h;
    "
}

export -f thread1
export -f thread2

clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &
clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread1 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &
clickhouse_client_loop_timeout 15 thread2 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table"
