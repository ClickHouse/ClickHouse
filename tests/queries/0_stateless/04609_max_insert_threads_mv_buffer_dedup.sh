#!/usr/bin/env bash
# Regression test: a dependent materialized view whose target forwards the write through a nested
# INSERT in a separate context (a Buffer, or a Distributed that forwards to one) must not let a plain
# INSERT fan out to max_insert_threads sink branches - not even when deduplication is disabled for the
# outer query. A Buffer flushes its accumulated data to the destination through a nested INSERT built
# from the buffer's own context (StorageBuffer::writeBlockToDestination copies the buffer's context,
# not the outer query context), so the outer query's deduplicate_insert / insert_deduplicate /
# deduplicate_blocks_in_dependent_materialized_views settings never reach that final write. When the
# MV target is such a Buffer, each parallel branch's BufferSink runs its own nested INSERT with the
# source block numbering restarted from zero, so identical blocks on different branches would collide
# on the deduplicating destination and rows would be silently dropped - even though the outer INSERT
# disabled deduplication. The dependent-MV fan-out must fail closed for a separate-context target
# regardless of the deduplication settings on the outer query, mirroring the top-level guard for a
# direct Buffer / Distributed target.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of parallel insert
# streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --parallel_view_processing=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_buf_dedup_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_buf_dedup_buffer"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_buf_dedup_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv_buf_dedup_src"

# src keeps every inserted block (no deduplication window); only the MV target's final destination dst
# deduplicates. The MV writes into a Buffer over dst, so the deduplicating write happens in the
# buffer's own flush context, out of reach of this query's settings.
$CLICKHOUSE_CLIENT -q "CREATE TABLE mv_buf_dedup_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mv_buf_dedup_dst (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mv_buf_dedup_buffer (x UInt64) ENGINE = Buffer(currentDatabase(), mv_buf_dedup_dst, 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mv_buf_dedup_mv TO mv_buf_dedup_buffer AS SELECT x FROM mv_buf_dedup_src"

# With dependent-MV deduplication enabled the Buffer target is already kept single-stream: a single
# BufferSink for the MV chain.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=1 -q \
    "EXPLAIN PIPELINE INSERT INTO mv_buf_dedup_src VALUES (1)" | grep -c "BufferSink"

# deduplicate_blocks_in_dependent_materialized_views = 0 must NOT relax the fan-out: the buffer's own
# flush context still deduplicates on dst, so the MV chain must stay single-stream. (This is the
# regression: before the fix this setting disabled the whole gate and the MV chain fanned out into
# several BufferSinks while the flush still deduplicated on dst, silently dropping rows.)
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=0 -q \
    "EXPLAIN PIPELINE INSERT INTO mv_buf_dedup_src VALUES (1)" | grep -c "BufferSink"

# Deduplication disabled for the outer query (deduplicate_insert = disable) must not relax it either.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO mv_buf_dedup_src VALUES (1)" | grep -c "BufferSink"

# Session-level insert_deduplicate = 0 must not relax it either.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --insert_deduplicate=0 -q \
    "EXPLAIN PIPELINE INSERT INTO mv_buf_dedup_src VALUES (1)" | grep -c "BufferSink"

# Row integrity through the MV into the Buffer into the deduplicating destination: four identical
# 100-row blocks must all arrive in dst after the buffer is flushed, even with deduplication disabled
# for the outer query. A per-branch fan-out would restart the numbering at zero, collide the ids of
# identical blocks across branches, and dst would silently drop rows. Kept intentionally small so the
# test stays well under the time limit under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --deduplicate_insert='disable' --min_insert_block_size_rows=100 --max_insert_block_size=100 \
    --max_block_size=100 -q "INSERT INTO mv_buf_dedup_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE mv_buf_dedup_buffer"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_buf_dedup_src), (SELECT count() FROM mv_buf_dedup_dst)"

# Positive control: when the MV target is a plain non-deduplicating table that writes in this query's
# context (not a separate-context Buffer / Distributed), the fan-out still applies - one MergeTreeSink
# for src and one for the MV target in each of the four branches.
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_buf_dedup_mv"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mv_buf_dedup_plain (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mv_buf_dedup_mv TO mv_buf_dedup_plain AS SELECT x FROM mv_buf_dedup_src"
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO mv_buf_dedup_src VALUES (1)" | grep -c "MergeTreeSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE mv_buf_dedup_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_buf_dedup_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_buf_dedup_buffer"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_buf_dedup_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE mv_buf_dedup_src"
