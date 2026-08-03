#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage

# Tests `merge_tree_min_bytes_per_read_stream`: the bytes-aware cap on the number of MergeTree
# read streams. For a narrow column the mark-based stream count is far larger than the data
# justifies, so the cap `ceil(sqrt(estimated_read_bytes / setting))` reduces it. The cap is only
# applied when the read volume can be estimated conservatively.
#
# Assertions are relations rather than exact stream counts, so they are stable across compression
# differences. The tags pin the pipeline shape: random merge-tree/object-storage settings change
# the read-stream layout, and the cap deliberately leaves remote reads alone.

# `max_threads_min_free_memory_per_thread` (1 GiB by default) lowers `max_threads` from the free
# memory of the global memory tracker at the moment a query is planned. On a busy server the two
# queries of a comparison get different values, which changes the stream count without the cap
# having anything to do with it. Disable it for every query of this test.
CLICKHOUSE_CLIENT_OPT="--max_threads_min_free_memory_per_thread=0"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Extract the read-stream multiplicity `MergeTreeSelect(...) × N` from EXPLAIN PIPELINE.
# `max(toUInt32OrZero(...))` collapses the (possibly several) pipeline lines to the single
# read-stream count, ignoring rows that do not carry a `× N` suffix.
stream_count() {
    $CLICKHOUSE_CLIENT -q "
        SELECT max(toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')))
        FROM (EXPLAIN PIPELINE $1)"
}

FORCE_STREAMS="max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_narrow"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_narrow (k UInt64, w UInt16) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
# Deterministic content (no rand) so the estimated size is stable.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_narrow SELECT number, number % 50000 FROM numbers(2000000)"

echo "-- the cap reduces streams for a narrow column --"
ON=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS $FORCE_STREAMS")
OFF=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")
[ "$ON" -lt "$OFF" ] && echo 1 || echo 0

echo "-- the cap never produces zero streams --"
[ "$ON" -ge 1 ] && echo 1 || echo 0

echo "-- results are identical with the cap on and off --"
RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64")
RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$RES_ON" = "$RES_OFF" ] && echo 1 || echo 0

# A column added with DEFAULT can require reading other physical columns from old parts. The
# estimator does not have the per-part dependency expansion available here, so it must leave the
# stream count unchanged instead of charging only the physically present requested columns.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_default_dependency"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_default_dependency (k UInt64, w UInt16, s String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_default_dependency SELECT number, number, repeat('x', 65536) FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_default_dependency ADD COLUMN d UInt64 DEFAULT length(s)"
echo "-- unknown DEFAULT dependencies do not cap streams --"
DEFAULT_ON=$(stream_count "SELECT sum(w), sum(d) FROM t_default_dependency SETTINGS $FORCE_STREAMS")
DEFAULT_OFF=$(stream_count "SELECT sum(w), sum(d) FROM t_default_dependency SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")
[ "$DEFAULT_ON" -eq "$DEFAULT_OFF" ] && echo 1 || echo 0

# A metadata-only `RENAME COLUMN` leaves the part holding the old name until the mutation is
# applied; the reader resolves the new name through `AlterConversions`. The estimator has to resolve
# it the same way, otherwise the cap silently stops applying to such a table.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_pending_rename"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_pending_rename (k UInt64, w UInt16) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_pending_rename SELECT number, number % 50000 FROM numbers(2000000)"
# Keep the mutation unapplied, so the part on disk still stores `w`.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_pending_rename"
$CLICKHOUSE_CLIENT --alter_sync 0 -q "ALTER TABLE t_pending_rename RENAME COLUMN w TO w2"
echo "-- the cap still applies through an unapplied RENAME COLUMN --"
RENAME_ON=$(stream_count "SELECT sum(w2) FROM t_pending_rename SETTINGS $FORCE_STREAMS")
RENAME_OFF=$(stream_count "SELECT sum(w2) FROM t_pending_rename SETTINGS $FORCE_STREAMS, merge_tree_min_bytes_per_read_stream = 0")
[ "$RENAME_ON" -lt "$RENAME_OFF" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_narrow"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_default_dependency"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_pending_rename"
