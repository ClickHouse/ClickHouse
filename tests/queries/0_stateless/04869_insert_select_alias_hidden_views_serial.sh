#!/usr/bin/env bash

# `parallel_view_processing = 0` must be honored by `INSERT SELECT` through a forwarding destination:
# an `Alias` hides its target's dependent-view graph behind the nested `INSERT` each `AliasSink` runs,
# so a fan-out to several sinks would push those hidden views concurrently across sibling branches.
# Such inserts must stay single-stream (one `AliasSink`), like the plain `INSERT` path already does.
# An `Alias` whose target has no dependent views keeps the `max_insert_threads` fan-out, and so does
# `parallel_view_processing = 1`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hidden_views_front"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hidden_views_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hidden_views_log"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alias_hidden_views_dst"

$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_hidden_views_dst (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_hidden_views_front ENGINE = Alias('alias_hidden_views_dst')"

echo "-- No dependent views on the target: the fan-out stays available"
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=0 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_hidden_views_front SELECT number FROM numbers(4)" | grep -c "AliasSink"

$CLICKHOUSE_CLIENT -q "CREATE TABLE alias_hidden_views_log (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW alias_hidden_views_mv TO alias_hidden_views_log AS SELECT x FROM alias_hidden_views_dst"

echo "-- Hidden dependent view, parallel_view_processing = 0: single stream"
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=0 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_hidden_views_front SELECT number FROM numbers(4)" | grep -c "AliasSink"

echo "-- Hidden dependent view, parallel_view_processing = 1: the fan-out stays available"
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO alias_hidden_views_front SELECT number FROM numbers(4)" | grep -c "AliasSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hidden_views_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hidden_views_log"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hidden_views_front"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_hidden_views_dst"
