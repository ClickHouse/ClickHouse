#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage

# A column added by `ALTER TABLE ... ADD COLUMN` is missing from the parts written before the
# `ALTER`. When its default expands to no physical identifiers - an explicit `DEFAULT 0` or the
# implicit type default - the reader fills it without reading anything, so the read set of such a
# part is still known and `merge_tree_min_bytes_per_read_stream` must keep capping the streams.
# Contrast with `04695_merge_tree_min_bytes_per_read_stream`, where the default reads a wide column
# and the cap is skipped.

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

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dependency_free_default"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_dependency_free_default (k UInt64, w UInt16)
    ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dependency_free_default SELECT number, number % 50000 FROM numbers(2000000)"

# The old part stores neither `c` nor `e`, and neither default reads a physical column.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dependency_free_default ADD COLUMN c UInt8 DEFAULT 0"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dependency_free_default ADD COLUMN e UInt8"

echo "-- the added columns are missing from the old part --"
$CLICKHOUSE_CLIENT -q "
    SELECT count() = 0 FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_dependency_free_default'
        AND column IN ('c', 'e') AND active"

echo "-- an explicit dependency-free DEFAULT still caps streams --"
ON=$(stream_count "SELECT sum(w), sum(c) FROM t_dependency_free_default SETTINGS $FORCE_STREAMS")
OFF=$(stream_count "SELECT sum(w), sum(c) FROM t_dependency_free_default SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")
[ "$ON" -lt "$OFF" ] && echo 1 || echo 0

echo "-- an implicit type default still caps streams --"
ON=$(stream_count "SELECT sum(w), sum(e) FROM t_dependency_free_default SETTINGS $FORCE_STREAMS")
OFF=$(stream_count "SELECT sum(w), sum(e) FROM t_dependency_free_default SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")
[ "$ON" -lt "$OFF" ] && echo 1 || echo 0

# Reading only the added column injects the minimum-size physical column, exactly like the reader.
echo "-- reading only the added column still caps streams --"
ON=$(stream_count "SELECT sum(c) FROM t_dependency_free_default SETTINGS $FORCE_STREAMS")
OFF=$(stream_count "SELECT sum(c) FROM t_dependency_free_default SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")
[ "$ON" -lt "$OFF" ] && echo 1 || echo 0

echo "-- results are identical with the cap on and off --"
RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w), sum(c), sum(e) FROM t_dependency_free_default SETTINGS max_threads = 64")
RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w), sum(c), sum(e) FROM t_dependency_free_default SETTINGS max_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$RES_ON" = "$RES_OFF" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dependency_free_default"
