#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "CREATE TABLE test_agg_proj (x Int32, y Int32, PROJECTION x_plus_y (SELECT sum(x - y), argMax(x, y) group by x + y)) ENGINE = MergeTree ORDER BY tuple() settings index_granularity = 1"
$CLICKHOUSE_CLIENT -q "insert into test_agg_proj select intDiv(number, 2), -intDiv(number,3) - 1 from numbers(100)"

$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select (x + y) * 2, sum(x - y) * 2 as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select (x + y) * 2, sum(x - y) * 2 as s from test_agg_proj group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select intDiv(x + y, 2), intDiv(x + y, 3), sum(x - y) as s from test_agg_proj group by intDiv(x + y, 2), intDiv(x + y, 3) order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select intDiv(x + y, 2), intDiv(x + y, 3), sum(x - y) as s from test_agg_proj group by intDiv(x + y, 2), intDiv(x + y, 3) order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select x + y + 1, argMax(x, y) * sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y + 1, argMax(x, y) * sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select x + y + 1, argMax(y, x), sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y + 1, argMax(y, x), sum(x - y) as s from test_agg_proj group by x + y + 1 order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj prewhere (x + y) % 2 = 1 group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1"
$CLICKHOUSE_CLIENT -q "select x + y, sum(x - y) as s from test_agg_proj prewhere (x + y) % 2 = 1 group by x + y order by s desc limit 5 settings allow_experimental_projection_optimization=1 format JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -q "drop table test_agg_proj"
