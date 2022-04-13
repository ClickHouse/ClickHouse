#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_read_in_order_2";

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_read_in_order_2(date Date, i UInt64, v UInt64) ENGINE = MergeTree ORDER BY (date, i)"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_read_in_order_2 SELECT '2020-10-10', number % 10, number FROM numbers(100000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_read_in_order_2 SELECT '2020-10-11', number % 10, number FROM numbers(100000)"

$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT date, i FROM t_read_in_order_2 WHERE date = '2020-10-11' OR date = '2020-10-12' ORDER BY i DESC LIMIT 10" | grep -o "MergeSorting"
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT date, i FROM t_read_in_order_2 WHERE date >= '2020-10-11' ORDER BY i DESC LIMIT 10" | grep -o "MergeSorting"
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT date, i FROM t_read_in_order_2 WHERE date = '2020-10-11' OR v = 100 ORDER BY i DESC LIMIT 10" | grep -o "MergeSorting"
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT date, i FROM t_read_in_order_2 WHERE date != '2020-10-11' ORDER BY i DESC LIMIT 10" | grep -o "MergeSorting"
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT date, i FROM t_read_in_order_2 WHERE NOT (date = '2020-10-11') ORDER BY i DESC LIMIT 10" | grep -o "MergeSorting"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_read_in_order_2";
