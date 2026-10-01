#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage

CLICKHOUSE_CLIENT_OPT="--max_threads_min_free_memory_per_thread=0 --enable_json_type=1"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

stream_count() {
    $CLICKHOUSE_CLIENT -q "
        SELECT max(toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')))
        FROM (EXPLAIN PIPELINE $1)"
}

FORCE_STREAMS="max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_subcolumn_stream_cap"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_subcolumn_stream_cap (k UInt64, j JSON(a UInt64, payload String))
    ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0"

# The requested `j.a` is fixed-size, but disabling subcolumn sizing makes the estimator fall back
# to the variable-width parent `j`. Concentrating the sibling payload in the selected range makes
# row-scaling that parent size non-conservative.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_subcolumn_stream_cap
    SELECT number,
        concat('{\"a\":', toString(number), ',\"payload\":\"', if(number < 128, repeat('x', 65536), ''), '\"}')
    FROM numbers(1024)"

ON=$(stream_count "SELECT sum(j.a) FROM t_subcolumn_stream_cap WHERE k < 128 SETTINGS $FORCE_STREAMS, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0")
OFF=$(stream_count "SELECT sum(j.a) FROM t_subcolumn_stream_cap WHERE k < 128 SETTINGS $FORCE_STREAMS, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0, merge_tree_min_bytes_per_read_stream = 0")
echo "-- the selective subcolumn read is wide enough for the cap to matter --"
[ "$OFF" -gt 16 ] && echo 1 || echo 0
echo "-- parent-column fallback skips the bytes-aware cap --"
[ "$ON" -eq "$OFF" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_subcolumn_stream_cap"
