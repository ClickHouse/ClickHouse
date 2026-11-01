#!/usr/bin/env bash
# Regression test that the strict plain-INSERT single-stream fallback is narrow: it only forces a
# single insert stream when the destination table actually deduplicates.
#
# max_insert_threads parallelizes the writing side of a plain INSERT by resizing the pipeline to
# several sink streams. With use_strict_insert_block_limits the deduplication info (source block
# number) is stamped after the fan-out, so a strict tokenized / token-less (under new_unified_hash)
# INSERT can produce colliding deduplication ids across branches. That collision only drops rows when
# the destination sink actually consults the ids, i.e. a MergeTree-family table with its deduplication
# window enabled and deduplication not disabled by deduplicate_insert / insert_deduplicate. For a table
# that never deduplicates the fan-out is safe and max_insert_threads must keep applying.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_nodedup_insert"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dedup_insert"

# A MergeTree with deduplication disabled (non_replicated_deduplication_window = 0, the default) never
# consults the deduplication block ids.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_nodedup_insert (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0"
# A MergeTree with deduplication enabled: used to show the fallback still fires when dedup is active.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dedup_insert (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 10000"

# Strict + tokenized INSERT into the no-dedup table still fans out: four sinks (before this fix the
# strict fallback fired unconditionally and forced a single sink).
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 --insert_deduplication_token='tok' -q \
    "EXPLAIN PIPELINE INSERT INTO t_nodedup_insert VALUES (1)" | grep -c "MergeTreeSink"

# Strict token-less INSERT into the no-dedup table under the default new_unified_hash also fans out:
# four sinks.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 -q \
    "EXPLAIN PIPELINE INSERT INTO t_nodedup_insert VALUES (1)" | grep -c "MergeTreeSink"

# Deduplication window enabled, but deduplication disabled for the session (deduplicate_insert=disable):
# no sink consults the ids, so a strict tokenized INSERT still fans out: four sinks.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 --insert_deduplication_token='tok' --deduplicate_insert='disable' -q \
    "EXPLAIN PIPELINE INSERT INTO t_dedup_insert VALUES (1)" | grep -c "MergeTreeSink"

# Control: deduplication window enabled AND deduplication active. The fallback still protects against
# per-branch id collisions dropping rows, so it keeps a single sink.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --use_strict_insert_block_limits=1 --insert_deduplication_token='tok' -q \
    "EXPLAIN PIPELINE INSERT INTO t_dedup_insert VALUES (1)" | grep -c "MergeTreeSink"

# Correctness: a strict tokenized INSERT into the no-dedup table, parallelized across four streams and
# split into many small source blocks, must keep all rows.
seq 1 100000 | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --use_strict_insert_block_limits=1 --insert_deduplication_token='data' \
    --min_insert_block_size_rows=1000 -q \
    "INSERT INTO t_nodedup_insert FORMAT TSV"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM t_nodedup_insert"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_nodedup_insert"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_dedup_insert"
