#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-distributed-cache
#
# no-fasttest -- needs an S3-backed disk
# no-random-settings -- pins use_reader_executor and reader_executor_window_size
# no-distributed-cache -- with distributed cache the executor falls back to the
#                         legacy path, emitting no ReaderExecutor source-byte counters

# A single-granule point read through the ReaderExecutor must pull about ONE
# granule from object storage, not a full reader_executor_window_size (8 MiB)
# window. The executor advertises the mark-range boundary to its live S3
# connection (setReadUntilPosition), so a `WHERE k = <one granule>` reads that
# granule's bytes and stops, instead of streaming an open-ended window past it.
# storage_policy='s3_no_cache' reads straight from S3 (no FileCache segment
# rounding), so the source-byte counter is exactly what the executor pulled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_bytes"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_bytes (k UInt64, v UInt64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS storage_policy = 's3_no_cache', index_granularity = 8192, min_bytes_for_wide_part = 0"
# v is random (incompressible): one granule ~ 8192*8 = 64 KiB on disk; 1M rows = 122 granules ~ 8 MiB.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_bytes SELECT number, sipHash64(number) FROM numbers(1000000)"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_re_bytes FINAL"

POINT="04306_point_${CLICKHOUSE_DATABASE}"
FULL="04306_full_${CLICKHOUSE_DATABASE}"

# One-granule point read (k = 530000 is mid-granule), and a full-column scan,
# both through the executor.
$CLICKHOUSE_CLIENT --use_reader_executor=1 --query_id "$POINT" --query "
    SELECT sum(v) FROM t_re_bytes WHERE k = 530000 FORMAT Null"
$CLICKHOUSE_CLIENT --use_reader_executor=1 --query_id "$FULL" --query "
    SELECT sum(v) FROM t_re_bytes FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# The point read pulls about one granule (~100 KiB: one 64 KiB granule of the
# random column plus the key granule and marks), far under both the 8 MiB window
# and the ~8 MiB full-part scan.
$CLICKHOUSE_CLIENT --query "
    WITH
        (SELECT ProfileEvents['ReaderExecutorBytesFromSource'] FROM system.query_log
         WHERE current_database = currentDatabase() AND query_id = '$POINT' AND type = 'QueryFinish'
         ORDER BY event_time_microseconds DESC LIMIT 1) AS point_bytes,
        (SELECT ProfileEvents['ReaderExecutorBytesFromSource'] FROM system.query_log
         WHERE current_database = currentDatabase() AND query_id = '$FULL' AND type = 'QueryFinish'
         ORDER BY event_time_microseconds DESC LIMIT 1) AS full_bytes
    SELECT
        point_bytes > 0 AS point_read_something,
        point_bytes < 1048576 AS point_about_one_granule,
        point_bytes * 4 < full_bytes AS point_much_smaller_than_full"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_bytes"
