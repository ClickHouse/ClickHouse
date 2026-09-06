#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With strict insert block limits, `AddDeduplicationInfoTransform` assigns source block numbers
# after the sink fan-out. A deduplicating destination must therefore keep an `INSERT SELECT`
# single-stream, otherwise identical blocks from sibling streams can receive the same id.
# Use the compatibility mode to enable deduplication for this intentionally unordered source.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS insert_select_deduplication_single_stream"
$CLICKHOUSE_CLIENT -q "CREATE TABLE insert_select_deduplication_single_stream (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 100"

$CLICKHOUSE_CLIENT --max_insert_threads 4 --max_threads 4 --use_strict_insert_block_limits 1 --deduplicate_insert_select enable_even_for_bad_queries -q \
    "EXPLAIN PIPELINE INSERT INTO insert_select_deduplication_single_stream SELECT number FROM numbers(4)" | grep -c "MergeTreeSink"

# Without strict limits, a direct destination receives globally numbered deduplication info before
# the sink fan-out, so the four writing streams stay available.
$CLICKHOUSE_CLIENT --max_insert_threads 4 --max_threads 4 --use_strict_insert_block_limits 0 --deduplicate_insert_select enable_even_for_bad_queries -q \
    "EXPLAIN PIPELINE INSERT INTO insert_select_deduplication_single_stream SELECT number FROM numbers(4)" | grep -c "MergeTreeSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE insert_select_deduplication_single_stream"
