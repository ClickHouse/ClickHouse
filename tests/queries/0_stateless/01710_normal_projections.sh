#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_sort_proj (x UInt32, y UInt32, PROJECTION p (SELECT x, y ORDER BY y)) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "insert into test_sort_proj select number, toUInt32(-number - 1) from numbers(100)"

echo "select where x < 10"

echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0"

echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1"

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0"

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1"

echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0" | grep rows_read

echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1" | grep rows_read

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0" | grep rows_read

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE x < 10 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1" | grep rows_read


echo "select where y > 4294967286"

echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0"

echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1"

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0"

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1"


echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 0" | grep rows_read

echo "optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 0, allow_experimental_projection_optimization = 1" | grep rows_read

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 0" | grep rows_read

echo "optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_sort_proj WHERE y > 4294967286 order by x FORMAT JSON
                       SETTINGS optimize_move_to_prewhere = 1, allow_experimental_projection_optimization = 1" | grep rows_read

$CLICKHOUSE_CLIENT -q "DROP TABLE test_sort_proj"
