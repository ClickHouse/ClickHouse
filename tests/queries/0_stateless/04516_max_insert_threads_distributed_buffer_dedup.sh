#!/usr/bin/env bash
# Regression test: a plain INSERT into a Distributed table must not fan out to max_insert_threads sink
# branches while its target path can deduplicate - not even when deduplication is disabled for the
# outer query. This query's deduplication settings do travel to the shard, but the shard's table is
# not cheaply known here and may itself be (or forward to) a Buffer. A Buffer flushes its accumulated
# data to the destination through a nested INSERT built from the buffer's own context
# (StorageBuffer::writeBlockToDestination copies the buffer's context, not the outer query context),
# so the outer query's deduplicate_insert / insert_deduplicate settings never reach that final write.
# With the fan-out the initiator builds several DistributedSinks, so one plain INSERT becomes several
# separate remote INSERT INTO buffer queries; each restarts the source block numbering from zero, so
# identical blocks on different branches would collide on the deduplicating destination and rows would
# be silently dropped inside one INSERT - even though the outer INSERT disabled deduplication. The
# fan-out must fail closed for a Distributed table regardless of the deduplication settings on the
# outer query.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of parallel insert
# streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dist_buf_dedup_dist"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dist_buf_dedup_buffer"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dist_buf_dedup_target"

$CLICKHOUSE_CLIENT -q "CREATE TABLE dist_buf_dedup_target (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dist_buf_dedup_buffer (x UInt64) ENGINE = Buffer(currentDatabase(), dist_buf_dedup_target, 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dist_buf_dedup_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_buf_dedup_buffer)"

# A Distributed over a Buffer over a deduplicating target with deduplication active: a single
# DistributedSink, because every branch would run its own remote INSERT with a per-branch block
# numbering.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO dist_buf_dedup_dist VALUES (1)" | grep -c "DistributedSink"

# Deduplication disabled for the outer query (deduplicate_insert = disable): the Distributed table
# must STILL be single-stream, because the remote Buffer's own flush context - not this query -
# governs whether the destination deduplicates. (This is the regression: before the fix the outer
# disable let the Distributed table fan out while the remote Buffer's flush still deduplicated on the
# destination, silently dropping rows.)
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO dist_buf_dedup_dist VALUES (1)" | grep -c "DistributedSink"

# Session-level insert_deduplicate = 0 must not relax the Distributed fan-out either.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --insert_deduplicate=0 -q \
    "EXPLAIN PIPELINE INSERT INTO dist_buf_dedup_dist VALUES (1)" | grep -c "DistributedSink"

# Row integrity through the Distributed into the Buffer into the deduplicating destination: four
# identical 100-row blocks must all arrive after the buffer is flushed, even with deduplication
# disabled for the outer query. Kept intentionally small so the test stays well under the time limit
# under the s3/keeper CI configuration.
for _ in $(seq 1 4); do seq 1 100; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --distributed_foreground_insert=1 --deduplicate_insert='disable' --min_insert_block_size_rows=100 \
    --max_insert_block_size=100 --max_block_size=100 -q "INSERT INTO dist_buf_dedup_dist FORMAT TSV"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE dist_buf_dedup_buffer"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM dist_buf_dedup_target"

$CLICKHOUSE_CLIENT -q "DROP TABLE dist_buf_dedup_dist"
$CLICKHOUSE_CLIENT -q "DROP TABLE dist_buf_dedup_buffer"
$CLICKHOUSE_CLIENT -q "DROP TABLE dist_buf_dedup_target"
