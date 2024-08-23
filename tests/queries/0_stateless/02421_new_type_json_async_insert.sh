#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_async_insert"
$CLICKHOUSE_CLIENT --allow_experimental_json_type=1 -q "CREATE TABLE t_json_async_insert (data JSON) ENGINE = MergeTree ORDER BY tuple()"

$CLICKHOUSE_CLIENT --async_insert=1 --wait_for_async_insert=1 -q 'INSERT INTO t_json_async_insert FORMAT JSONAsObject {"aaa"}' 2>&1 | grep -o -m1 "INCORRECT_DATA"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_json_async_insert"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 't_json_async_insert'"

$CLICKHOUSE_CLIENT --async_insert=1 --wait_for_async_insert=1 -q 'INSERT INTO t_json_async_insert FORMAT JSONAsObject {"aaa"}' 2>&1 | grep -o -m1 "INCORRECT_DATA" &
$CLICKHOUSE_CLIENT --async_insert=1 --wait_for_async_insert=1 -q 'INSERT INTO t_json_async_insert FORMAT JSONAsObject {"k1": "aaa"}' &

wait

$CLICKHOUSE_CLIENT -q "SELECT data.k1 FROM t_json_async_insert ORDER BY data.k1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_async_insert"
