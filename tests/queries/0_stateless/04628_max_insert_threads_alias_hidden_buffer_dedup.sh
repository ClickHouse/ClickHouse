#!/usr/bin/env bash
# Regression test: a dependent materialized view hidden behind an Alias hop whose target forwards the
# write into a separate context (a Buffer, or a Distributed) must not let a plain INSERT fan out to
# max_insert_threads sink branches. An INSERT INTO an Alias of a table with a `mv TO Buffer(dst)`
# expands that view graph only inside the nested INSERT each AliasSink runs, so with
# parallel_view_processing = 1 the outer pipeline would build several AliasSinks; each branch's nested
# INSERT then reaches a BufferSink that drops the carried deduplication info, and the buffer flushes
# to the destination through a fresh INSERT built from the buffer's own context with the source block
# numbering restarted from zero. Identical blocks from different outer branches would then collide on
# the deduplicating destination and rows would be silently dropped - even when the outer query
# disabled deduplication, because those settings never reach the separate-context write. The insert
# must stay single-stream in this topology, mirroring the guard for the visible
# `src -> mv TO Buffer(dst)` variant.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of parallel insert
# streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --parallel_view_processing=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_buffer"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_src"

# src keeps every inserted block (no deduplication window); only the hidden MV target's final
# destination dst deduplicates, and the MV writes into a Buffer over dst, so the deduplicating write
# happens in the buffer's own flush context, out of reach of the outer query's settings.
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_buf_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_buf_dst (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_buf_buffer (x UInt64) ENGINE = Buffer(currentDatabase(), alias_buf_dst, 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_buf_alias ENGINE = Alias('alias_buf_src')"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW alias_buf_mv TO alias_buf_buffer AS SELECT x FROM alias_buf_src"

# The hidden `src -> mv TO Buffer(dst)` chain must keep the outer INSERT single-stream: one AliasSink,
# regardless of the deduplication settings on the outer query. (This is the regression: before the fix
# the outer pipeline fanned out into four AliasSinks while the buffer flush still deduplicated on dst,
# silently dropping rows.)
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO alias_buf_alias VALUES (1)" | grep -c "AliasSink"

# Disabled dependent-MV deduplication must not relax it either: the buffer's own flush context still
# deduplicates on dst.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --insert_deduplicate=1 --deduplicate_blocks_in_dependent_materialized_views=0 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_buf_alias VALUES (1)" | grep -c "AliasSink"

# Session-level insert_deduplicate = 0 must not relax it either.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --insert_deduplicate=0 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_buf_alias VALUES (1)" | grep -c "AliasSink"

# Row integrity through the Alias into the hidden MV into the Buffer into the deduplicating
# destination: four identical 100-row blocks must all arrive in dst after the buffer is flushed, even
# with deduplication disabled for the outer query. Kept intentionally small so the test stays well
# under the time limit under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --deduplicate_insert='disable' --min_insert_block_size_rows=100 --max_insert_block_size=100 \
    --max_block_size=100 -q "INSERT INTO alias_buf_alias FORMAT TSV"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE alias_buf_buffer"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM alias_buf_src), (SELECT count() FROM alias_buf_dst)"

# The same hidden hop reached through a *visible* dependent view: INSERT INTO src2 where
# `src2 -> mv2 TO alias2` and alias2's target has its own `mv TO Buffer(dst)`. The dependent target
# alias2 is visible to InsertDependenciesBuilder, but the Buffer sits in the view graph hidden behind
# alias2's nested INSERT, so the builder must keep its sink stream size at 1: one AliasSink.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_alias2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_buf_src2"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_buf_src2 (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_buf_alias2 ENGINE = Alias('alias_buf_src')"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW alias_buf_mv2 TO alias_buf_alias2 AS SELECT x FROM alias_buf_src2"
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO alias_buf_src2 VALUES (1)" | grep -c "AliasSink"

# Positive control: when the hidden MV target is a plain non-deduplicating table that writes in this
# query's context (not a separate-context Buffer / Distributed), the fan-out still applies: four
# AliasSinks.
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_mv"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_buf_plain (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW alias_buf_mv TO alias_buf_plain AS SELECT x FROM alias_buf_src"
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO alias_buf_alias VALUES (1)" | grep -c "AliasSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_mv2"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_alias2"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_src2"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_buffer"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_buf_src"
