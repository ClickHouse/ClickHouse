#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage

# Tests `merge_tree_min_bytes_per_read_stream`: the bytes-aware cap on the
# number of MergeTree read streams. For a narrow column the mark-based stream count is
# far larger than the data justifies, so the cap `ceil(sqrt(estimated_read_bytes / setting))`
# reduces it. Assertions are expressed as relations (not exact stream counts) so they are
# stable across compression differences. The tags pin the pipeline shape: random
# merge-tree/object-storage settings change the read-stream layout.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_narrow"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_narrow (k UInt64, w UInt16) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
# A single narrow column, deterministic content (no rand) so the size is stable.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_narrow SELECT number, number % 50000 FROM numbers(2000000)"

# Extract the read-stream multiplicity `MergeTreeSelect(...) × N` from EXPLAIN PIPELINE.
# `max(toUInt32OrZero(...))` collapses the (possibly several) pipeline lines to the single
# read-stream count, ignoring rows that do not carry a `× N` suffix.
stream_count() {
    $CLICKHOUSE_CLIENT -q "
        SELECT max(toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')))
        FROM (EXPLAIN PIPELINE $1)"
}

ON=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1")
OFF=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1, merge_tree_min_bytes_per_read_stream = 0")
SMALL=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 4")

echo "-- cap reduces streams for a narrow column --"
[ "$ON" -lt "$OFF" ] && echo 1 || echo 0
echo "-- cap never produces zero streams --"
[ "$ON" -ge 1 ] && echo 1 || echo 0
# A narrow pipeline is left alone entirely: the per-stream overhead the cap removes only dominates
# once the pipeline is wide, so there is an absolute floor below which no reduction happens.
echo "-- a narrow pipeline is never capped --"
[ "$SMALL" -eq 4 ] && echo 1 || echo 0

echo "-- results are identical with the cap on and off --"
RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64")
RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$RES_ON" = "$RES_OFF" ] && echo 1 || echo 0

# The estimate scales by the fraction of marks left after primary key pruning, so a selective
# query gets fewer streams than a full scan of the same column.
echo "-- selective read gets fewer streams than a full scan --"
RANGE_FULL=$(stream_count "SELECT sum(w) FROM t_narrow WHERE k < 2000000 SETTINGS max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1")
RANGE_PARTIAL=$(stream_count "SELECT sum(w) FROM t_narrow WHERE k < 1000000 SETTINGS max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1")
[ "$RANGE_PARTIAL" -lt "$RANGE_FULL" ] && echo 1 || echo 0

# Compact parts do not track per-column sizes, so the estimate falls back to the whole part.
# The cap must still apply (rather than being silently skipped) and still return correct results.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_compact"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_compact (k UInt64, w UInt16) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_compact SELECT number, number % 50000 FROM numbers(2000000)"
echo "-- compact parts: cap still applies --"
C_ON=$(stream_count "SELECT sum(w) FROM t_compact SETTINGS max_threads = 64")
C_OFF=$(stream_count "SELECT sum(w) FROM t_compact SETTINGS max_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$C_ON" -lt "$C_OFF" ] && echo 1 || echo 0
echo "-- compact parts: results are identical with the cap on and off --"
C_RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_compact SETTINGS max_threads = 64")
C_RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_compact SETTINGS max_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$C_RES_ON" = "$C_RES_OFF" ] && echo 1 || echo 0

# A highly compressible column is tiny on disk but still feeds every row through aggregation,
# so the estimate uses uncompressed size and must not collapse the read to a handful of streams.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hicomp"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_hicomp (k UInt64, c UInt64 CODEC(ZSTD(9))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_hicomp SELECT number, 1 FROM numbers(2000000)"
echo "-- highly compressible column is sized by uncompressed bytes --"
# Compressed size is tiny, while the uncompressed column is 16 MB.
H_ON=$(stream_count "SELECT sum(c) FROM t_hicomp SETTINGS max_threads = 64")
[ "$H_ON" -gt 8 ] && echo 1 || echo 0

# Subcolumns of one physical column must never be charged more than the physical column itself:
# each subcolumn is sized on its own where that is allowed, and the enclosing column is charged at
# most once otherwise. Double counting the parent per subcolumn would exceed the whole-column read.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_sub"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_sub (k UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_sub SELECT number, map('a', number, 'b', number) FROM numbers(500000)"
echo "-- subcolumns sharing a physical column are charged once when exact sizes are disabled --"
SUB_TWO=$(stream_count "SELECT sum(arraySum(arrayMap(x -> length(x), m.keys))), sum(arraySum(m.values)) FROM t_sub SETTINGS max_threads = 64, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0")
SUB_ONE=$(stream_count "SELECT sum(arraySum(arrayMap(x -> length(x), m.keys))) FROM t_sub SETTINGS max_threads = 64, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0")
[ "$SUB_TWO" -eq "$SUB_ONE" ] && echo 1 || echo 0
echo "-- exact sizing never charges one subcolumn more than the physical column --"
SUB_ONE_EXACT=$(stream_count "SELECT sum(arraySum(arrayMap(x -> length(x), m.keys))) FROM t_sub SETTINGS max_threads = 64, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1")
[ "$SUB_ONE_EXACT" -le "$SUB_TWO" ] && echo 1 || echo 0

# FINAL nests one merging pipeline per primary key range, so the `× N` multiplicity of a single
# line does not carry the stream count there. Sum the read processors across all lines instead.
total_read_processors() {
    $CLICKHOUSE_CLIENT -q "
        SELECT sum(if(match(explain, 'MergeTreeSelect.*× (\\d+)'), toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')), 1))
        FROM (EXPLAIN PIPELINE $1)
        WHERE explain LIKE '%MergeTreeSelect%'"
}

# FINAL uses a separate stream-spreading path, which the cap also covers.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_final"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_final (k UInt64, w UInt16) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_final SELECT number, number % 50000 FROM numbers(1000000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_final SELECT number, number % 50000 FROM numbers(1000000)"
echo "-- FINAL: cap applies and results are identical --"
F_ON=$(total_read_processors "SELECT sum(w) FROM t_final FINAL SETTINGS max_threads = 64, max_final_threads = 64")
F_OFF=$(total_read_processors "SELECT sum(w) FROM t_final FINAL SETTINGS max_threads = 64, max_final_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$F_ON" -lt "$F_OFF" ] && echo 1 || echo 0
F_RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_final FINAL SETTINGS max_threads = 64, max_final_threads = 64")
F_RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_final FINAL SETTINGS max_threads = 64, max_final_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$F_RES_ON" = "$F_RES_OFF" ] && echo 1 || echo 0

# Reading in order uses yet another path.
echo "-- read in order: cap applies and results are identical --"
O_ON=$(stream_count "SELECT w FROM t_narrow ORDER BY k SETTINGS max_threads = 64, optimize_read_in_order = 1, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1")
O_OFF=$(stream_count "SELECT w FROM t_narrow ORDER BY k SETTINGS max_threads = 64, optimize_read_in_order = 1, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1, merge_tree_min_bytes_per_read_stream = 0")
[ "$O_ON" -lt "$O_OFF" ] && echo 1 || echo 0
O_RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM (SELECT w FROM t_narrow ORDER BY k) SETTINGS max_threads = 64, optimize_read_in_order = 1")
O_RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM (SELECT w FROM t_narrow ORDER BY k) SETTINGS max_threads = 64, optimize_read_in_order = 1, merge_tree_min_bytes_per_read_stream = 0")
[ "$O_RES_ON" = "$O_RES_OFF" ] && echo 1 || echo 0

# Many parts: the estimate sums over all of them.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_parts"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_parts (k UInt64, w UInt16) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0"
for _ in {1..4}; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_parts SELECT number, number % 50000 FROM numbers(500000)"
done
echo "-- many parts: cap applies and results are identical --"
P_ON=$(stream_count "SELECT sum(w) FROM t_parts SETTINGS max_threads = 64")
P_OFF=$(stream_count "SELECT sum(w) FROM t_parts SETTINGS max_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$P_ON" -lt "$P_OFF" ] && echo 1 || echo 0
P_RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_parts SETTINGS max_threads = 64")
P_RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_parts SETTINGS max_threads = 64, merge_tree_min_bytes_per_read_stream = 0")
[ "$P_RES_ON" = "$P_RES_OFF" ] && echo 1 || echo 0

# Parallel replicas plan over all selected parts on every node, but each node reads only its share.
# Use a smaller byte-equivalent cost so both the single-node and per-node estimates stay above the
# 16-stream floor: this makes the division by the participating node count directly observable.
LOCAL_FOR_PR=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1, merge_tree_min_bytes_per_read_stream = 4096")
PARALLEL_REPLICAS_SETTINGS="max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1, enable_analyzer = 1, parallel_replicas_only_with_analyzer = 0, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1"
PR_ON=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS $PARALLEL_REPLICAS_SETTINGS, merge_tree_min_bytes_per_read_stream = 4096")
PR_OFF=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS $PARALLEL_REPLICAS_SETTINGS, merge_tree_min_bytes_per_read_stream = 0")
echo "-- parallel replicas: cap reduces streams --"
[ "$PR_ON" -lt "$PR_OFF" ] && echo 1 || echo 0
echo "-- parallel replicas: per-node estimate reduces the cap --"
[ "$PR_ON" -lt "$LOCAL_FOR_PR" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_narrow"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_compact"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_hicomp"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_sub"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_final"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_parts"
