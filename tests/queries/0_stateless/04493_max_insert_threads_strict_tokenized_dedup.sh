#!/usr/bin/env bash
# Regression test for the strict tokenized plain-INSERT fallback.
#
# max_insert_threads parallelizes the writing side of a plain INSERT by resizing the
# pipeline to several sink streams. With use_strict_insert_block_limits the deduplication
# info (source block number) is stamped by a per-stream AddDeduplicationInfoTransform after
# the fan-out, so each parallel branch restarts its block numbering from zero. Together with
# a non-empty insert_deduplication_token (whose dedup id is `token` + source block number,
# independent of the block contents) that produces identical ids across branches, which
# MergeTreeSink treats as duplicates and skips - silently dropping rows of a single INSERT.
# Such inserts must therefore fall back to a single insert stream.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_strict_tokenized_insert"
# Deduplication must be active so that a regression (removing the single-stream fallback)
# would actually drop rows and fail this test.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_strict_tokenized_insert (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"

# The strict + tokenized combination falls back to a single insert stream: one sink.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 --insert_deduplication_token='tok' -q \
    "EXPLAIN PIPELINE INSERT INTO t_strict_tokenized_insert VALUES (1)" | grep -c "MergeTreeSink"

# The single-stream fallback is gated on use_strict_insert_block_limits: a tokenized insert without
# strict limits stamps the source block number in the single-stream head, before the fan-out, so it can
# still parallelize the writing side. Four sinks. (The strict token-less default new_unified_hash case,
# which must also fall back, is covered by 04494.)
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --insert_deduplication_token='tok' -q \
    "EXPLAIN PIPELINE INSERT INTO t_strict_tokenized_insert VALUES (1)" | grep -c "MergeTreeSink"

# All rows must arrive under the strict + tokenized combination, even though the input is
# split into many small source blocks and max_insert_threads > 1. If the fallback were
# removed, per-stream block numbering would collide across branches and dedup would drop rows.
seq 1 100000 | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --use_strict_insert_block_limits=1 --insert_deduplication_token='data' \
    --min_insert_block_size_rows=1000 -q \
    "INSERT INTO t_strict_tokenized_insert FORMAT TSV"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM t_strict_tokenized_insert"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_strict_tokenized_insert"
