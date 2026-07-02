#!/usr/bin/env bash
# Regression test for the strict token-less plain-INSERT fallback.
#
# max_insert_threads parallelizes the writing side of a plain INSERT by resizing the pipeline to
# several sink streams. With use_strict_insert_block_limits the deduplication info (source block
# number) is stamped by a per-stream AddDeduplicationInfoTransform after the fan-out, so each parallel
# branch restarts its block numbering from zero.
#
# Under the default insert_deduplication_version = new_unified_hash the block id of a token-less block
# is hash(data_hash : source_block_number) for a synchronous insert, so the source block number is part
# of the id. Two identical squashed blocks that land on different branches then both get source number 0
# and the same data hash, i.e. an identical id, which MergeTreeSink treats as a duplicate and skips -
# silently dropping rows of a single INSERT. Such inserts must therefore fall back to a single insert
# stream (only insert_deduplication_version = old_separate_hashes derives a token-less id from the data
# hash alone and can safely fan out).
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_strict_tokenless_insert"
# Deduplication must be active so that a regression (removing the single-stream fallback)
# would actually drop rows and fail this test.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_strict_tokenless_insert (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"

# The strict token-less INSERT falls back to a single insert stream under the default
# insert_deduplication_version = new_unified_hash: one sink.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO t_strict_tokenless_insert VALUES (1)" | grep -c "MergeTreeSink"

# Without strict block limits the source block number is stamped in the single-stream head, before the
# fan-out, so the token-less INSERT still parallelizes its writing side: four sinks. This confirms the
# fallback is narrow and the main max_insert_threads win is preserved.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO t_strict_tokenless_insert VALUES (1)" | grep -c "MergeTreeSink"

# All rows must arrive. The input is ten identical 1000-row blocks (the values 1..1000 repeated), and
# strict block limits squash it into ten identical blocks. Under new_unified_hash those ten blocks are
# kept only because their source block numbers (0..9) differ; if the fan-out restarted numbering per
# branch, identical blocks would collide on source number 0 and dedup would drop rows.
for _ in $(seq 1 10); do seq 1 1000; done | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --use_strict_insert_block_limits=1 \
    --min_insert_block_size_rows=1000 --max_insert_block_size=1000 --max_block_size=1000 -q \
    "INSERT INTO t_strict_tokenless_insert FORMAT TSV"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM t_strict_tokenless_insert"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_strict_tokenless_insert"
