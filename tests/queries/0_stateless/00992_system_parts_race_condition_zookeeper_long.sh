#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS alter_table0;
    DROP TABLE IF EXISTS alter_table1;

    CREATE TABLE alter_table0 (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r1') ORDER BY a PARTITION BY b % 10
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration=0, replicated_max_mutations_in_one_entry = $(($RANDOM / 50 + 100));
    CREATE TABLE alter_table1 (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r2') ORDER BY a PARTITION BY b % 10
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration=0, replicated_max_mutations_in_one_entry = $(($RANDOM / 50 + 200));
"

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    while true; do $CLICKHOUSE_CLIENT --query "SELECT * FROM system.parts FORMAT Null"; done
}

function thread2()
{
    while true; do $CLICKHOUSE_CLIENT --query "ALTER TABLE alter_table0 ADD COLUMN h String DEFAULT '0'; ALTER TABLE alter_table0 MODIFY COLUMN h UInt64; ALTER TABLE alter_table0 DROP COLUMN h;"; done
}

function thread3()
{
    while true; do $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table0 SELECT rand(1), rand(2), 1 / rand(3), toString(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(100000)"; done
}

function thread4()
{
    while true; do $CLICKHOUSE_CLIENT --receive_timeout=1 -q "OPTIMIZE TABLE alter_table0 FINAL" | grep -Fv "Timeout exceeded while receiving data from server"; done
}

function thread5()
{
    while true; do $CLICKHOUSE_CLIENT -q "ALTER TABLE alter_table0 DELETE WHERE cityHash64(a,b,c,d,e,g) % 1048576 < 524288"; done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;
export -f thread3;
export -f thread4;
export -f thread5;

TIMEOUT=10

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread2 2> /dev/null &
timeout $TIMEOUT bash -c thread3 2> /dev/null &
timeout $TIMEOUT bash -c thread4 2> /dev/null &
timeout $TIMEOUT bash -c thread5 2> /dev/null &

wait
check_replication_consistency "alter_table" "count(), sum(a), sum(b), round(sum(c))"

$CLICKHOUSE_CLIENT -q "SELECT table, lost_part_count FROM system.replicas WHERE database=currentDatabase() AND lost_part_count!=0";

$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table0;" 2> >(grep -F -v 'is already started to be removing by another replica right now') &
$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table1;" 2> >(grep -F -v 'is already started to be removing by another replica right now') &
wait
