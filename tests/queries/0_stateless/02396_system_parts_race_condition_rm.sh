#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel, disabled

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

set -e

# NOTE this test is copy of 00992_system_parts_race_condition_zookeeper_long, but with extra thread7

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS alter_table0;
    DROP TABLE IF EXISTS alter_table1;

    CREATE TABLE alter_table0 (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r1') ORDER BY a PARTITION BY b % 10
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration=0;
    CREATE TABLE alter_table1 (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r2') ORDER BY a PARTITION BY b % 10
    SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration=0
"

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM system.parts FORMAT Null"
    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -n --query "ALTER TABLE alter_table0 ADD COLUMN h String DEFAULT '0'; ALTER TABLE alter_table0 MODIFY COLUMN h UInt64; ALTER TABLE alter_table0 DROP COLUMN h;"
    done
}

function thread3()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table0 SELECT rand(1), rand(2), 1 / rand(3), toString(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(100000)"
    done
}

function thread4()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE alter_table0 FINAL"
    done
}

function thread5()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "ALTER TABLE alter_table0 DELETE WHERE cityHash64(a,b,c,d,e,g) % 1048576 < 524288"
    done
}

function thread7()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database='$CLICKHOUSE_DATABASE' AND table LIKE 'alter_table%' ORDER BY rand() LIMIT 1")
        if [ -z "$path" ]; then continue; fi
        # ensure that path is absolute before removing
        $CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path') format Null" || exit
        rm -rf $path 2> /dev/null
        sleep 0.$RANDOM;
    done
}

TIMEOUT=10

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &

thread1 2> /dev/null &
thread2 2> /dev/null &
thread3 2> /dev/null &
thread4 2> /dev/null &
thread5 2> /dev/null &

thread7 &

wait
check_replication_consistency "alter_table" "count(), sum(a), sum(b), round(sum(c))"

$CLICKHOUSE_CLIENT -n -q "DROP TABLE alter_table0;" 2> >(grep -F -v 'is already started to be removing by another replica right now') &
$CLICKHOUSE_CLIENT -n -q "DROP TABLE alter_table1;" 2> >(grep -F -v 'is already started to be removing by another replica right now') &
wait
