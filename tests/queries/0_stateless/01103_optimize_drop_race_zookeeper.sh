#!/usr/bin/env bash
# Tags: race, zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

function thread1()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO concurrent_optimize_table SELECT rand(1), rand(2), 1 / rand(3), toString(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(10000)"
}


function thread2()
{
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE concurrent_optimize_table FINAL"
    sleep 0.$RANDOM
}

function thread3()
{
    $CLICKHOUSE_CLIENT -mn -q "
        DROP TABLE IF EXISTS concurrent_optimize_table;
        CREATE TABLE concurrent_optimize_table (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_optimize_table', '1')
        ORDER BY a
        PARTITION BY b % 10
        SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0;";
    sleep 0.$RANDOM
    sleep 0.$RANDOM
    sleep 0.$RANDOM
}


export -f thread1
export -f thread2
export -f thread3


TIMEOUT=15

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS concurrent_optimize_table"
