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

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dropped_column_stream_cap"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_dropped_column_stream_cap (k UInt64, w UInt16, s String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_dropped_column_stream_cap
    SELECT number, number % 50000, repeat('x', 256) FROM numbers(200000)"

# Keep the mutation unapplied, so the part on disk still stores the narrow `w`, while the re-added
# `w` of the same name is a default expression over the wide `s` that the reader actually reads.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_dropped_column_stream_cap"
$CLICKHOUSE_CLIENT --alter_sync 0 -q "ALTER TABLE t_dropped_column_stream_cap DROP COLUMN w"
$CLICKHOUSE_CLIENT --alter_sync 0 -q "ALTER TABLE t_dropped_column_stream_cap ADD COLUMN w UInt64 DEFAULT length(s)"

echo "-- the stale narrow column is still stored in the part --"
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_dropped_column_stream_cap'
        AND column = 'w' AND type = 'UInt16' AND active"

ON=$(stream_count "SELECT sum(w) FROM t_dropped_column_stream_cap SETTINGS $FORCE_STREAMS")
OFF=$(stream_count "SELECT sum(w) FROM t_dropped_column_stream_cap SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")

echo "-- the read is wide enough for the cap to matter --"
[ "$OFF" -gt 16 ] && echo 1 || echo 0
echo "-- a pending DROP COLUMN of the same name skips the bytes-aware cap --"
[ "$ON" -eq "$OFF" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dropped_column_stream_cap"
