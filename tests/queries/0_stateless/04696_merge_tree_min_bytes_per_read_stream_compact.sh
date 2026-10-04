#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage

CLICKHOUSE_CLIENT_OPT="--max_threads_min_free_memory_per_thread=0"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

stream_count() {
    $CLICKHOUSE_CLIENT -q "
        SELECT max(toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')))
        FROM (EXPLAIN PIPELINE $1)"
}

FORCE_STREAMS="max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_compact_stream_cap"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_compact_stream_cap (k UInt64, w UInt16, payload String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1, min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000"

# Concentrate the wide values in the selected range. Row-scaling the shared compact-part file would
# underestimate this range, so partial reads must skip the cap when per-column sizes are unavailable.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_compact_stream_cap
    SELECT number, number, if(number < 128, repeat('x', 65536), '') FROM numbers(1024)"

echo "-- the test part uses compact storage --"
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 AND count() = countIf(part_type = 'Compact')
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_compact_stream_cap' AND active"

ON=$(stream_count "SELECT sum(w) FROM t_compact_stream_cap WHERE k < 128 SETTINGS $FORCE_STREAMS")
OFF=$(stream_count "SELECT sum(w) FROM t_compact_stream_cap WHERE k < 128 SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")
echo "-- the selective read is wide enough for the cap to matter --"
[ "$OFF" -gt 16 ] && echo 1 || echo 0
echo "-- selective compact-part reads skip the bytes-aware cap --"
[ "$ON" -eq "$OFF" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_compact_stream_cap"
