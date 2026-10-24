#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Like `05355_insert_select_buffer_plain_destination_fan_out`, but the `Buffer` is reached through a
# forwarding hop: an `Alias` over it, or a materialized view `TO` it. A `Buffer` over a destination that
# neither deduplicates nor has dependent views keeps the `max_insert_threads` fan-out there as well; a
# deduplicating destination keeps a single sink.

SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0 --parallel_view_processing=1"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE plain_dst (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0;
    CREATE TABLE plain_buf (x UInt32) ENGINE = Buffer(currentDatabase(), plain_dst, 1, 0, 0, 10000, 10000, 0, 0);
    CREATE TABLE dedup_dst (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 100;
    CREATE TABLE dedup_buf (x UInt32) ENGINE = Buffer(currentDatabase(), dedup_dst, 1, 0, 0, 10000, 10000, 0, 0);
    CREATE TABLE plain_alias ENGINE = Alias(plain_buf);
    CREATE TABLE dedup_alias ENGINE = Alias(dedup_buf);
    CREATE TABLE plain_src (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0;
    CREATE MATERIALIZED VIEW plain_mv TO plain_buf AS SELECT x FROM plain_src;
    CREATE TABLE dedup_src (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0;
    CREATE MATERIALIZED VIEW dedup_mv TO dedup_buf AS SELECT x FROM dedup_src;
"

echo "-- Alias over a Buffer with a plain destination"
$CLICKHOUSE_CLIENT $SETTINGS -q "EXPLAIN PIPELINE INSERT INTO plain_alias SELECT number FROM numbers(4)" | grep -c "AliasSink"
echo "-- Alias over a Buffer with a deduplicating destination"
$CLICKHOUSE_CLIENT $SETTINGS -q "EXPLAIN PIPELINE INSERT INTO dedup_alias SELECT number FROM numbers(4)" | grep -c "AliasSink"
echo "-- materialized view to a Buffer with a plain destination"
$CLICKHOUSE_CLIENT $SETTINGS -q "EXPLAIN PIPELINE INSERT INTO plain_src SELECT number FROM numbers(4)" | grep -c "BufferSink"
echo "-- materialized view to a Buffer with a deduplicating destination"
$CLICKHOUSE_CLIENT $SETTINGS -q "EXPLAIN PIPELINE INSERT INTO dedup_src SELECT number FROM numbers(4)" | grep -c "BufferSink"

echo "-- all rows arrive through the parallel writes"
$CLICKHOUSE_CLIENT $SETTINGS --max_block_size=1 --min_insert_block_size_rows=1 -q "INSERT INTO plain_alias SELECT number FROM numbers(50)"
$CLICKHOUSE_CLIENT $SETTINGS --max_block_size=1 --min_insert_block_size_rows=1 -q "INSERT INTO plain_src SELECT number FROM numbers(50)"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM plain_dst"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE dedup_mv;
    DROP TABLE dedup_src;
    DROP TABLE plain_mv;
    DROP TABLE plain_src;
    DROP TABLE dedup_alias;
    DROP TABLE plain_alias;
    DROP TABLE dedup_buf;
    DROP TABLE dedup_dst;
    DROP TABLE plain_buf;
    DROP TABLE plain_dst;
"
