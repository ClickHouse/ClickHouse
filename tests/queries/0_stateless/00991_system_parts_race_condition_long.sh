#!/usr/bin/env bash
# Tags: race, no-parallel
# no-parallel because we run many concurrent mutations, which may break other tests by delaying their
# mutations for a long time.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alter_table (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16)) ENGINE = MergeTree ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1"

TIMEOUT=30

function thread1()
{
    # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --max-execution-time 10 --query "SELECT * FROM system.parts FORMAT Null" 2> /dev/null
    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --max-execution-time 10 --query "ALTER TABLE alter_table ADD COLUMN h String '0'; ALTER TABLE alter_table MODIFY COLUMN h UInt64; ALTER TABLE alter_table DROP COLUMN h;" 2> /dev/null
    done
}

function thread3()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --max-execution-time 10 -q "INSERT INTO alter_table SELECT rand(1), rand(2), 1 / rand(3), toString(rand(4)), [rand(5), rand(6)], rand(7) % 2 ? NULL : generateUUIDv4(), (rand(8), rand(9)) FROM numbers(1000)" 2> /dev/null
    done
}

function thread4()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --max-execution-time 10 -q "OPTIMIZE TABLE alter_table FINAL" 2> /dev/null
    done
}

function thread5()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --mutations-sync 1 --max-execution-time 10 -q "ALTER TABLE alter_table DELETE WHERE rand() % 2 = 1" 2> /dev/null
    done
}

thread1 &
thread2 &
thread3 &
thread4 &
thread5 &

thread1 &
thread2 &
thread3 &
thread4 &
thread5 &

thread1 &
thread2 &
thread3 &
thread4 &
thread5 &

thread1 &
thread2 &
thread3 &
thread4 &
thread5 &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table"
