#!/usr/bin/env bash
# Tags: race
# Regression test for debug assertion in calculateEachColumnSizes.
#
# The assertion fires when a numeric column is declared in columns.txt but has
# no data file on disk. This legitimately happens when concurrent ALTER
# ADD/MODIFY/DROP COLUMN and mutations produce a part with such a mismatch.
#
# In Debug builds WITHOUT the fix, this throws LOGICAL_ERROR.
# With the fix (or in Release), it succeeds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS column_sizes_race"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE column_sizes_race (a UInt8, b Int16, c Float32, d String)
    ENGINE = MergeTree ORDER BY a PARTITION BY b % 10
    SETTINGS old_parts_lifetime = 1
"

TIMEOUT=15

function thread_insert()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10" -d "INSERT INTO column_sizes_race SELECT rand(1), rand(2), 1 / rand(3), toString(rand(4)) FROM numbers(1000)" >& /dev/null
    done
}

function thread_alter_column()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10" -d "ALTER TABLE column_sizes_race ADD COLUMN h String DEFAULT '0'" >& /dev/null
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10" -d "ALTER TABLE column_sizes_race MODIFY COLUMN h UInt64" >& /dev/null
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10" -d "ALTER TABLE column_sizes_race DROP COLUMN h" >& /dev/null
    done
}

function thread_delete()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10&mutations_sync=1" -d "ALTER TABLE column_sizes_race DELETE WHERE rand() % 2 = 1" >& /dev/null
    done
}

function thread_optimize()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10" -d "OPTIMIZE TABLE column_sizes_race FINAL" >& /dev/null
    done
}

function thread_query_parts()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10" -d "SELECT * FROM system.parts WHERE database = currentDatabase() FORMAT Null" >& /dev/null
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_execution_time=10" -d "SELECT * FROM system.parts_columns WHERE database = currentDatabase() FORMAT Null" >& /dev/null
    done
}

thread_insert &
thread_alter_column &
thread_delete &
thread_optimize &
thread_query_parts &

thread_insert &
thread_alter_column &
thread_delete &
thread_optimize &
thread_query_parts &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE column_sizes_race"
echo "OK"
