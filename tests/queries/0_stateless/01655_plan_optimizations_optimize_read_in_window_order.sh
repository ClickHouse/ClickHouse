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

$CLICKHOUSE_CLIENT -q "drop table ${name}"
$CLICKHOUSE_CLIENT -q "drop table ${name}_n"
$CLICKHOUSE_CLIENT -q "drop table ${name}_n_x"
