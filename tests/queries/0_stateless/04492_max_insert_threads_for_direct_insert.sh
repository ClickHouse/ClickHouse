#!/usr/bin/env bash
# Tests that max_insert_threads parallelizes the writing side of a plain INSERT,
# i.e. data sent from clickhouse-client or over the HTTP interface, not just INSERT SELECT.
# See https://github.com/ClickHouse/ClickHouse/issues/108997

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_direct_insert_threads"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_direct_insert_threads (x UInt64) ENGINE = MergeTree ORDER BY x"

# With max_insert_threads = 1 the writing side of a plain INSERT stays single-threaded: one sink.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=1 -q \
    "EXPLAIN PIPELINE INSERT INTO t_direct_insert_threads VALUES (1)" | grep -c "MergeTreeSink"

# With max_insert_threads = 4 the pipeline is resized to 4 parallel insert streams: four sinks.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO t_direct_insert_threads VALUES (1)" | grep -c "MergeTreeSink"

# The data is inserted correctly when the writing side is parallelized.
# Over the native protocol (clickhouse-client), streaming the data - not INSERT SELECT.
seq 1 1000 | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "INSERT INTO t_direct_insert_threads FORMAT TSV"

# Over the HTTP interface.
seq 1001 2000 | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&max_threads=8&max_insert_threads=4&max_threads_min_free_memory_per_thread=0&max_insert_threads_min_free_memory_per_thread=0&query=INSERT+INTO+t_direct_insert_threads+FORMAT+TSV" \
    --data-binary @-

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x), min(x), max(x) FROM t_direct_insert_threads"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_direct_insert_threads"
