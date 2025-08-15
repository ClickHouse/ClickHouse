#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: FIXME: Timing issues with INSERT + DETACH (https://github.com/ClickHouse/ClickHouse/pull/67610/files#r1700345054)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NEW_DATABASE=test_01107_${CLICKHOUSE_DATABASE}
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${NEW_DATABASE} ENGINE=Atomic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${NEW_DATABASE}.mt (n UInt64) ENGINE=MergeTree() ORDER BY tuple()"

$CLICKHOUSE_CLIENT --function_sleep_max_microseconds_per_block 60000000 -q "INSERT INTO ${NEW_DATABASE}.mt SELECT number + sleepEachRow(3) FROM numbers(5)" &
sleep 1

$CLICKHOUSE_CLIENT -q "DETACH TABLE ${NEW_DATABASE}.mt" --database_atomic_wait_for_drop_and_detach_synchronously=0
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${NEW_DATABASE}.mt" --database_atomic_wait_for_drop_and_detach_synchronously=0 2>&1 | grep -F "Code: 57" > /dev/null && echo "OK"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${NEW_DATABASE}" --database_atomic_wait_for_drop_and_detach_synchronously=0 2>&1 | grep -F "Code: 219" > /dev/null && echo "OK"

wait
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${NEW_DATABASE}.mt"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM ${NEW_DATABASE}.mt"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${NEW_DATABASE}" --database_atomic_wait_for_drop_and_detach_synchronously=0
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${NEW_DATABASE}"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM ${NEW_DATABASE}.mt"

$CLICKHOUSE_CLIENT --function_sleep_max_microseconds_per_block 60000000 -q "INSERT INTO ${NEW_DATABASE}.mt SELECT number + sleepEachRow(1) FROM numbers(5)" && echo "end" &
sleep 1
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${NEW_DATABASE}" --database_atomic_wait_for_drop_and_detach_synchronously=0 && sleep 1 && echo "dropped"
wait
