#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

name=test_01655_plan_optimizations_optimize_read_in_window_order

$CLICKHOUSE_CLIENT -q "drop table if exists ${name}"
$CLICKHOUSE_CLIENT -q "drop table if exists ${name}_n"
$CLICKHOUSE_CLIENT -q "drop table if exists ${name}_n_x"

$CLICKHOUSE_CLIENT -q "create table ${name} engine=MergeTree order by tuple() as select toInt64((sin(number)+2)*65535)%10 as n, number as x from numbers_mt(100000)"
$CLICKHOUSE_CLIENT -q "create table ${name}_n engine=MergeTree order by n as select * from ${name} order by n"
$CLICKHOUSE_CLIENT -q "create table ${name}_n_x engine=MergeTree order by (n, x) as select * from ${name} order by n, x"

$CLICKHOUSE_CLIENT -q "optimize table ${name}_n final"
$CLICKHOUSE_CLIENT -q "optimize table ${name}_n_x final"

echo 'Partial sorting plan'
echo '  optimize_read_in_window_order=0'
$CLICKHOUSE_CLIENT -q "explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n SETTINGS optimize_read_in_window_order=0" | grep -i "sort description"

echo '  optimize_read_in_window_order=1'
$CLICKHOUSE_CLIENT -q "explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n SETTINGS optimize_read_in_window_order=1" | grep -i "sort description"

echo 'No sorting plan'
echo '  optimize_read_in_window_order=0'
$CLICKHOUSE_CLIENT -q "explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n_x SETTINGS optimize_read_in_window_order=0" | grep -i "sort description"

echo '  optimize_read_in_window_order=1'
$CLICKHOUSE_CLIENT -q "explain plan actions=1, description=1 select n, sum(x) OVER (ORDER BY n, x ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) from ${name}_n_x SETTINGS optimize_read_in_window_order=1" | grep -i "sort description"

echo 'Complex ORDER BY'
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${name}_complex (unique1 Int32, unique2 Int32, ten Int32) ENGINE=MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_complex VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)"
echo '  optimize_read_in_window_order=0'
$CLICKHOUSE_CLIENT -q "SELECT ten, sum(unique1) + sum(unique2) AS res, rank() OVER (ORDER BY sum(unique1) + sum(unique2) ASC) AS rank FROM ${name}_complex GROUP BY ten ORDER BY ten ASC SETTINGS optimize_read_in_window_order=0"
echo '  optimize_read_in_window_order=1'
$CLICKHOUSE_CLIENT -q "SELECT ten, sum(unique1) + sum(unique2) AS res, rank() OVER (ORDER BY sum(unique1) + sum(unique2) ASC) AS rank FROM ${name}_complex GROUP BY ten ORDER BY ten ASC SETTINGS optimize_read_in_window_order=1"

$CLICKHOUSE_CLIENT -q "drop table ${name}"
$CLICKHOUSE_CLIENT -q "drop table ${name}_n"
$CLICKHOUSE_CLIENT -q "drop table ${name}_n_x"
$CLICKHOUSE_CLIENT -q "drop table ${name}_complex"
