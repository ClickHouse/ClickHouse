#!/usr/bin/env bash
# Tags: no-fasttest, long, no-parallel
# Disable parallel since it creates 10 different threads querying and might overload the server

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table t(a UInt32, b UInt32, c UInt32) engine=MergeTree order by (a, b, c) settings index_granularity=1;"
$CLICKHOUSE_CLIENT -q "system stop merges t;"

# In this part a only changes 10% of the time, b 50% of the time, c all the time
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 10), intDiv(number, 2), number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 10), intDiv(number, 2), number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 10), intDiv(number, 2), number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 10), intDiv(number, 2), number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 10), intDiv(number, 2), number from numbers_mt(100);"

# In this part a only changes 33% of the time, b 50% of the time, c 10 % of the time
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 3), intDiv(number, 2), intDiv(number, 3) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 3), intDiv(number, 2), intDiv(number, 3) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 3), intDiv(number, 2), intDiv(number, 3) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 3), intDiv(number, 2), intDiv(number, 3) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t select intDiv(number, 3), intDiv(number, 2), intDiv(number, 3) from numbers_mt(100);"

# In this part a changes 100% of the time
$CLICKHOUSE_CLIENT -q "insert into t Select number, number, number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select number, number, number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select number, number, number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select number, number, number from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select number, number, number from numbers_mt(100);"


# In this part a changes 100% of the time
$CLICKHOUSE_CLIENT -q "insert into t Select 0, intDiv(number, 10), intDiv(number, 2) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select 0, intDiv(number, 10), intDiv(number, 2) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select 0, intDiv(number, 10), intDiv(number, 2) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select 0, intDiv(number, 10), intDiv(number, 2) from numbers_mt(100);"
$CLICKHOUSE_CLIENT -q "insert into t Select 0, intDiv(number, 10), intDiv(number, 2) from numbers_mt(100);"

function thread_alter_settings()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --query "ALTER TABLE t MODIFY SETTING primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns=0.$RANDOM"
        $CLICKHOUSE_CLIENT --query "SYSTEM UNLOAD PRIMARY KEY t"
        sleep 0.0$RANDOM
    done
}

function thread_query_table()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        COUNT=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM t where not ignore(*);")
        if [ "$COUNT" -ne "2000" ];  then
          echo "$COUNT"
        fi
    done
}

export -f thread_alter_settings
export -f thread_query_table

TIMEOUT=10

thread_alter_settings $TIMEOUT &
for _ in $(seq 1 10);
do
  thread_query_table $TIMEOUT &
done

wait

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t FORMAT Null"
