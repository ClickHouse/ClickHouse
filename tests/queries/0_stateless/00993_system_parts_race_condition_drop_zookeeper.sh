#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel, no-backward-compatibility-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

set -e

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    $CLICKHOUSE_CLIENT --query "SELECT * FROM system.parts FORMAT Null"
}

function thread2()
{
    REPLICA=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -n --query "ALTER TABLE alter_table_$REPLICA ADD COLUMN h String '0'; ALTER TABLE alter_table_$REPLICA MODIFY COLUMN h UInt64; ALTER TABLE alter_table_$REPLICA DROP COLUMN h;"
}

function thread3()
{
    REPLICA=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table_$REPLICA SELECT rand(1), rand(2), 1 / rand(3), toString(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(100000)"
}

function thread4()
{
    REPLICA=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE alter_table_$REPLICA FINAL"
    sleep 0.$RANDOM
}

function thread5()
{
    REPLICA=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -q "ALTER TABLE alter_table_$REPLICA DELETE WHERE cityHash64(a,b,c,d,e,g) % 1048576 < 524288"
    sleep 0.$RANDOM
}

function thread6()
{
    REPLICA=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -mn -q "
        DROP TABLE IF EXISTS alter_table_$REPLICA;

        CREATE TABLE alter_table_$REPLICA (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r_$REPLICA')
        ORDER BY a
        PARTITION BY b % 10
        SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0;
    "
    sleep 0.$RANDOM
}


export -f thread1
export -f thread2
export -f thread3
export -f thread4
export -f thread5
export -f thread6

TIMEOUT=30

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread5 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread6 2>&1 | grep "was not completely removed from ZooKeeper" &

wait

check_replication_consistency "alter_table_" "count(), sum(a), sum(b), round(sum(c))"

for i in {0..9}; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table_$i" 2>&1 | grep "was not completely removed from ZooKeeper" &
done

wait
