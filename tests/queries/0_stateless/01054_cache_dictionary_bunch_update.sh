#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="drop table if exists test_01054;"

$CLICKHOUSE_CLIENT --query="create table test_01054
                            (key UInt64, i8 Int8, i16 Int16, i32 Int32, i64 Int64, u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64)
                            Engine = Memory;"

$CLICKHOUSE_CLIENT --query="insert into test_01054 values (1, 1, 1, 1, 1, 1, 1, 1, 1);"
$CLICKHOUSE_CLIENT --query="insert into test_01054 values (2, 2, 2, 2, 2, 2, 2, 2, 2);"
$CLICKHOUSE_CLIENT --query="insert into test_01054 values (3, 3, 3, 3, 3, 3, 3, 3, 3);"

function thread1()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  for _ in {1..100}
  do
    [ $SECONDS -lt "$TIMELIMIT" ] || break
    RAND_NUMBER_THREAD1=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD1));"
  done
}


function thread2()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  for _ in {1..100}
  do
    [ $SECONDS -lt "$TIMELIMIT" ] || break
    RAND_NUMBER_THREAD2=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD2));"
  done
}


function thread3()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  for _ in {1..100}
  do
    [ $SECONDS -lt "$TIMELIMIT" ] || break
    RAND_NUMBER_THREAD3=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD3));"
  done
}


function thread4()
{
  local TIMELIMIT=$((SECONDS+TIMEOUT))
  for _ in {1..100}
  do
    [ $SECONDS -lt "$TIMELIMIT" ] || break
    RAND_NUMBER_THREAD4=$($CLICKHOUSE_CLIENT --query="SELECT rand() % 100;")
    $CLICKHOUSE_CLIENT --query="select dictGet('one_cell_cache_ints', 'i8', toUInt64($RAND_NUMBER_THREAD4));"
  done
}


TIMEOUT=10

# shellcheck disable=SC2188
thread1 > /dev/null 2>&1 &
thread2 > /dev/null 2>&1 &
thread3 > /dev/null 2>&1 &
thread4 > /dev/null 2>&1 &

wait

echo OK

$CLICKHOUSE_CLIENT --query "DROP TABLE if exists test_01054"
