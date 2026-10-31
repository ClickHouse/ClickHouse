#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `INSERT SELECT` into a `Buffer` keeps the `max_insert_threads` fan-out when the destination of the
# buffer neither deduplicates nor has dependent views: every direct write and every flush of the buffer
# is its own nested `INSERT`, so the number of sinks cannot make deduplication ids collide, and there are
# no hidden views to serialize. A destination with deduplication or a dependent view keeps a single sink.

SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0 --parallel_view_processing=0"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE plain_dst (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0;
    CREATE TABLE plain_buf (x UInt32) ENGINE = Buffer(currentDatabase(), plain_dst, 1, 0, 0, 10000, 10000, 0, 0);
    CREATE TABLE dedup_dst (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 100;
    CREATE TABLE dedup_buf (x UInt32) ENGINE = Buffer(currentDatabase(), dedup_dst, 1, 0, 0, 10000, 10000, 0, 0);
    CREATE TABLE views_dst (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0;
    CREATE TABLE views_buf (x UInt32) ENGINE = Buffer(currentDatabase(), views_dst, 1, 0, 0, 10000, 10000, 0, 0);
    CREATE TABLE views_log (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW views_mv TO views_log AS SELECT x FROM views_dst;
"

echo "-- plain destination"
$CLICKHOUSE_CLIENT $SETTINGS -q "EXPLAIN PIPELINE INSERT INTO plain_buf SELECT number FROM numbers(4)" | grep -c "BufferSink"
echo "-- deduplicating destination"
$CLICKHOUSE_CLIENT $SETTINGS -q "EXPLAIN PIPELINE INSERT INTO dedup_buf SELECT number FROM numbers(4)" | grep -c "BufferSink"
echo "-- destination with a dependent view"
$CLICKHOUSE_CLIENT $SETTINGS -q "EXPLAIN PIPELINE INSERT INTO views_buf SELECT number FROM numbers(4)" | grep -c "BufferSink"

echo "-- all rows arrive through the parallel direct writes"
$CLICKHOUSE_CLIENT $SETTINGS --max_block_size=1 --min_insert_block_size_rows=1 -q "INSERT INTO plain_buf SELECT number FROM numbers(50)"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM plain_dst"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE views_mv;
    DROP TABLE views_log;
    DROP TABLE views_buf;
    DROP TABLE views_dst;
    DROP TABLE dedup_buf;
    DROP TABLE dedup_dst;
    DROP TABLE plain_buf;
    DROP TABLE plain_dst;
"
