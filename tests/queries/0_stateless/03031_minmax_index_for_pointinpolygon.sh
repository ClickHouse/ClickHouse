#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists minmax_index_point_in_polygon"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE minmax_index_point_in_polygon
(
  x UInt32,
  y UInt32,
  INDEX mm_x_y (x, y) TYPE minmax GRANULARITY 1
) 
ENGINE = MergeTree()
ORDER BY x 
SETTINGS index_granularity = 3"

$CLICKHOUSE_CLIENT -q "Insert into minmax_index_point_in_polygon values
  (4, 4),
  (6 ,6),
  (8, 8),
  (12, 12),
  (14, 14),
  (16, 16)"

function query_and_check()
{
    query="$1"
    $CLICKHOUSE_CLIENT -q "$query"
    $CLICKHOUSE_CLIENT -q "$query FORMAT JSON" | grep "rows_read"
}

# 1/2 marks filtered by minmax index, read_rows should be 3
query_and_check "select * from minmax_index_point_in_polygon where pointInPolygon((x, y), [(4., 4.), (8., 4.), (8., 8.), (4., 8.)])"
query_and_check "select * from minmax_index_point_in_polygon where pointInPolygon((x, y), [(10., 13.), (14., 14.), (14, 10)])"


# 2/2 marks filtered by minmax index, read_rows should be 0
query_and_check "select * from minmax_index_point_in_polygon where pointInPolygon((x, y), [(0., 0.), (2., 2.), (2., 0.)])"

$CLICKHOUSE_CLIENT -q "drop table if exists minmax_index_point_in_polygon"
