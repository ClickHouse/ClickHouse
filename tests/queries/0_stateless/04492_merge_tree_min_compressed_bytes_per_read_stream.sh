#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas, no-object-storage

# Tests `merge_tree_min_compressed_bytes_per_read_stream`: the bytes-aware cap on the
# number of MergeTree read streams. For a narrow column the mark-based stream count is
# far larger than the data justifies, so the cap `sqrt(total_compressed_bytes / setting)`
# reduces it. Assertions are expressed as relations (not exact stream counts) so they are
# stable across compression differences. The tags pin the pipeline shape: random
# merge-tree/object-storage/parallel-replicas settings change the read-stream layout.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_narrow"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_narrow (w UInt16) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
# A single narrow column, deterministic content (no rand) so the compressed size is stable.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_narrow SELECT number % 50000 FROM numbers(20000000)"

# Extract the read-stream multiplicity `MergeTreeSelect(...) × N` from EXPLAIN PIPELINE.
# `max(toUInt32OrZero(...))` collapses the (possibly several) pipeline lines to the single
# read-stream count, ignoring rows that do not carry a `× N` suffix.
stream_count() {
    $CLICKHOUSE_CLIENT -q "
        SELECT max(toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')))
        FROM (EXPLAIN PIPELINE $1)"
}

ON=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64")
OFF=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64, merge_tree_min_compressed_bytes_per_read_stream = 0")
SMALL=$(stream_count "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 4")

echo "-- cap reduces streams for a narrow column --"
[ "$ON" -lt "$OFF" ] && echo 1 || echo 0
echo "-- disabled (=0) keeps the full max_threads --"
[ "$OFF" -eq 64 ] && echo 1 || echo 0
echo "-- cap never produces zero streams --"
[ "$ON" -ge 1 ] && echo 1 || echo 0
echo "-- cap does not raise streams above max_threads when max_threads is already small --"
[ "$SMALL" -eq 4 ] && echo 1 || echo 0

echo "-- results are identical with the cap on and off --"
RES_ON=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64")
RES_OFF=$($CLICKHOUSE_CLIENT -q "SELECT sum(w) FROM t_narrow SETTINGS max_threads = 64, merge_tree_min_compressed_bytes_per_read_stream = 0")
[ "$RES_ON" = "$RES_OFF" ] && echo 1 || echo 0

$CLICKHOUSE_CLIENT -q "DROP TABLE t_narrow"
