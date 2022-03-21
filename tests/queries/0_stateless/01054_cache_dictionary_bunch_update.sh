#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="create database if not exists test_01054;"
$CLICKHOUSE_CLIENT --query="drop table if exists test_01054.ints;"

$CLICKHOUSE_CLIENT --query="create table test_01054.ints
                            (key UInt64, i8 Int8, i16 Int16, i32 Int32, i64 Int64, u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64)
                            Engine = Memory;"

$CLICKHOUSE_CLIENT --query="insert into test_01054.ints values (1, 1, 1, 1, 1, 1, 1, 1, 1);"
$CLICKHOUSE_CLIENT --query="insert into test_01054.ints values (2, 2, 2, 2, 2, 2, 2, 2, 2);"
$CLICKHOUSE_CLIENT --query="insert into test_01054.ints values (3, 3, 3, 3, 3, 3, 3, 3, 3);"

function thread1()
{
    RAND_NUMBER_THREAD1=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD1));"
}


function thread2()
{
    RAND_NUMBER_THREAD2=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD2));"
}


function thread3()
{
    RAND_NUMBER_THREAD3=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD3));"
}


function thread4()
{
    RAND_NUMBER_THREAD4=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD4));"
}


export -f thread1
export -f thread2
export -f thread3
export -f thread4

TIMEOUT=10

# shellcheck disable=SC2188
clickhouse_client_loop_timeout $TIMEOUT thread1 > /dev/null 2>&1 &
clickhouse_client_loop_timeout $TIMEOUT thread2 > /dev/null 2>&1 &
clickhouse_client_loop_timeout $TIMEOUT thread3 > /dev/null 2>&1 &
clickhouse_client_loop_timeout $TIMEOUT thread4 > /dev/null 2>&1 &

wait

echo OK

$CLICKHOUSE_CLIENT --query "DROP TABLE if exists test_01054.ints"
$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01054"
