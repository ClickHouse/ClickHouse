#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `parallel_view_processing = 0` must be honored by `INSERT SELECT` into a `Buffer` table.
# The buffer writes its destination through a nested `INSERT`, hiding that table's dependent-view
# graph from the outer `InsertDependenciesBuilder`; therefore sibling `BufferSink`s would otherwise
# run separate nested view graphs concurrently.

SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS buffer_hidden_views_front"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS buffer_hidden_views_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS buffer_hidden_views_log"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS buffer_hidden_views_dst"

$CLICKHOUSE_CLIENT -q "CREATE TABLE buffer_hidden_views_dst (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE buffer_hidden_views_front (x UInt32) ENGINE = Buffer(currentDatabase(), buffer_hidden_views_dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE buffer_hidden_views_log (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW buffer_hidden_views_mv TO buffer_hidden_views_log AS SELECT x FROM buffer_hidden_views_dst"

echo "-- Hidden dependent view behind a Buffer, parallel_view_processing = 0: single stream"
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=0 -q \
    "EXPLAIN PIPELINE INSERT INTO buffer_hidden_views_front SELECT number FROM numbers(4)" | grep -c "BufferSink"

echo "-- Hidden dependent view behind a Buffer, parallel_view_processing = 1: the separate-context write stays single-stream"
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO buffer_hidden_views_front SELECT number FROM numbers(4)" | grep -c "BufferSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE buffer_hidden_views_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE buffer_hidden_views_log"
$CLICKHOUSE_CLIENT -q "DROP TABLE buffer_hidden_views_front"
$CLICKHOUSE_CLIENT -q "DROP TABLE buffer_hidden_views_dst"
