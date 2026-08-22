#!/usr/bin/env bash
# Regression test for the dependent-MV deduplication fallback of parallel plain INSERTs.
#
# max_insert_threads parallelizes the writing side of a plain INSERT by resizing the pipeline to
# several sink streams; with parallel_view_processing enabled this also fans out the dependent
# materialized-view chains. The view-level deduplication ids fold in the source block number, so they
# stay distinct across branches as long as the source numbering is global (stamped by the
# single-stream head of the pipeline before the fan-out). With use_strict_insert_block_limits the
# source block number is stamped per branch after the fan-out, so two identical source blocks landing
# on different branches produce identical view-level ids, and a deduplicating MV target silently drops
# one of them. Such inserts must keep the dependent-MV deduplication path single-stream; when no
# dependent target deduplicates the fan-out stays safe and max_insert_threads keeps applying.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --parallel_view_processing=1 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1"

create_tables()
{
    local dst_dedup_window=$1
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_dedup_fanout_mv"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_dedup_fanout_dst"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_dedup_fanout_src"
    # src keeps every inserted block (no deduplication window); only the MV target dst may
    # deduplicate. This isolates the dependent-MV deduplication path from source-table deduplication.
    $CLICKHOUSE_CLIENT -q "CREATE TABLE mv_dedup_fanout_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE mv_dedup_fanout_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = $dst_dedup_window"
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mv_dedup_fanout_mv TO mv_dedup_fanout_dst AS SELECT x FROM mv_dedup_fanout_src"
}

create_tables 100000

# Without strict limits the source numbering is global, so the fan-out is safe even though the MV
# target deduplicates: a MergeTreeSink for src and one for dst in each of the four branches.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO mv_dedup_fanout_src VALUES (1)" | grep -c "MergeTreeSink"

# With strict limits the numbering is per branch and the MV target deduplicates: single stream,
# one MergeTreeSink for src plus one for dst.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO mv_dedup_fanout_src VALUES (1)" | grep -c "MergeTreeSink"

# All rows must arrive. The input is four identical 100-row blocks; per-branch numbering would
# collide their view-level ids across branches and dst would silently lose blocks. Kept intentionally
# small so the test stays well under the time limit under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO mv_dedup_fanout_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_dedup_fanout_src), (SELECT count() FROM mv_dedup_fanout_dst)"

# The same with strict limits (single stream keeps the numbering global).
create_tables 100000
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --use_strict_insert_block_limits=1 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO mv_dedup_fanout_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_dedup_fanout_src), (SELECT count() FROM mv_dedup_fanout_dst)"

# When no dependent target deduplicates (dst with the window disabled), the per-branch numbering is
# never consulted, so even a strict insert fans out to four branches.
create_tables 0
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO mv_dedup_fanout_src VALUES (1)" | grep -c "MergeTreeSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE mv_dedup_fanout_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_dedup_fanout_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_dedup_fanout_src"
