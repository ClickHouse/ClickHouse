#!/usr/bin/env bash

# `CachedOnDiskReadBufferFromFile` must not emit TEST-level log lines once per buffer refill:
# assert the TEST log count stays tiny while many cached reads happen.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `cache` disk over `local_disk` needs no object storage. randomPrintableASCII keeps the data
# incompressible, so the compressed reads are large enough to need many buffer refills.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_cached_read_log;
    CREATE TABLE t_cached_read_log (k UInt64, s String)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_cache',
                         path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_cache',
                         max_size = '512Mi', max_file_segment_size = '4Mi',
                         boundary_alignment = '1Mi', cache_on_write_operations = 1,
                         load_metadata_asynchronously = 0, disk = 'local_disk'),
             min_bytes_for_wide_part = 0;
    INSERT INTO t_cached_read_log SELECT number, randomPrintableASCII(200) FROM numbers(100000)
    SETTINGS max_insert_threads = 1;
"

# Every setting below is randomized by tests/clickhouse-test and each one can either make the
# test vacuous or fail it on a correct binary, so they are pinned in the query itself
# (a query-level SETTINGS clause wins over the runner's injection):
#   enable_filesystem_cache=1                                   -- 0 never builds the cache buffer
#   read_from_filesystem_cache_if_exists_otherwise_bypass_cache=0 -- 1 takes the bypass branch
#   use_uncompressed_cache=0                                    -- 1 routes through another class
#   max_read_buffer_size/max_read_buffer_size_local_fs           -- small buffer => many refills
#   filesystem_cache_prefer_bigger_buffer_size=0                -- otherwise the buffer is widened
#   filesystem_cache_segments_batch_size=1                      -- makes the batch hold one segment,
#                                                                  so the per-refill resize log fires
#   min_bytes_to_use_direct_io / _mmap_io=0, local_filesystem_read_method='pread',
#   local_filesystem_read_prefetch=0, max_threads=1              -- keep the cached read path
read_settings="enable_filesystem_cache = 1, max_read_buffer_size = 4096,
    max_read_buffer_size_local_fs = 4096, use_uncompressed_cache = 0,
    read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0,
    min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0,
    local_filesystem_read_method = 'pread', local_filesystem_read_prefetch = 0,
    filesystem_cache_prefer_bigger_buffer_size = 0,
    filesystem_cache_segments_batch_size = 1, max_threads = 1"

# Warm the cache so the measured query reads from it (ReadType::CACHED) rather than downloading.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(cityHash64(s)), sum(k) FROM t_cached_read_log SETTINGS $read_settings
" > /dev/null

query_id="cached_read_log_${CLICKHOUSE_DATABASE}_$$"

# send_logs_level=test makes the server evaluate the TEST-level logs (and hence the LogTest
# ProfileEvent) regardless of the server's own log level.
$CLICKHOUSE_CLIENT --send_logs_level=test --query_id="$query_id" -q "
    SELECT sum(cityHash64(s)), sum(k) FROM t_cached_read_log SETTINGS $read_settings
" > /dev/null 2>/dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# With the fix, LogTest is a handful (buffer-open + seek lines), while thousands of cached reads
# happen. Without the fix, LogTest ~= 3 * cached reads (>> 1000).
# many_reads is what stops the test passing vacuously when nothing was read from the cache.
$CLICKHOUSE_CLIENT -q "
    SELECT
        ProfileEvents['CachedReadBufferReadFromCacheHits'] > 1000 AS many_reads,
        ProfileEvents['LogTest'] < 1000 AS few_test_logs
    FROM system.query_log
    WHERE query_id = '$query_id' AND current_database = currentDatabase()
      AND type = 'QueryFinish' AND read_rows > 0
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cached_read_log;"
