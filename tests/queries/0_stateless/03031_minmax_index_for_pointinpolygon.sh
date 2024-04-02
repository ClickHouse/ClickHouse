#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists t3"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t3(x UInt32, y UInt32, INDEX mm_x_y (x, y) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 3;"
$CLICKHOUSE_CLIENT -q "Insert into t3 values(4, 4), (6 ,6), (8, 8), (12, 12), (14, 14), (16, 16);"

$CLICKHOUSE_CLIENT -q "Truncate table system.query_log sync;"

function query_and_check()
{
    query_id="$1"
    query="$2"
    $CLICKHOUSE_CLIENT --query_id $query_id -q "$query settings log_queries = 1"
    $CLICKHOUSE_CLIENT -q "system flush logs"
    $CLICKHOUSE_CLIENT -q "select query_id, read_rows from system.query_log where current_database = currentDatabase() and query_id = '$query_id' and type='QueryFinish'"
}

# 1/2 marks filtered by minmax index, read_rows should be 3
query_and_check "minmax_index_for_pointinpolygon_1" "select * from t3 where pointInPolygon((x, y), [(4., 4.), (8., 4.), (8., 8.), (4., 8.)])"
query_and_check "minmax_index_for_pointinpolygon_2" "select * from t3 where pointInPolygon((x, y), [(10., 13.), (14., 14.), (14, 10)])"


# 2/2 marks filtered by minmax index, read_rows should be 0
query_and_check "minmax_index_for_pointinpolygon_3" "select * from t3 where pointInPolygon((x, y), [(0., 0.), (2., 2.), (2., 0.)])"

$CLICKHOUSE_CLIENT -q "drop table if exists t3"
