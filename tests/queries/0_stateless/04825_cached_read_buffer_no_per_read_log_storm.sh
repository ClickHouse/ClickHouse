#!/usr/bin/env bash

# `CachedOnDiskReadBufferFromFile` must not emit TEST-level log lines once per buffer refill unless
# `filesystem_cache_verbose_logging` is on. Both directions are asserted: with the setting off the
# TEST log count stays tiny while many cached reads happen, with it on the messages come back.

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
             min_bytes_for_wide_part = 0, index_granularity = 8192;
    INSERT INTO t_cached_read_log SELECT number, randomPrintableASCII(200) FROM numbers(100000)
    SETTINGS max_insert_threads = 1;
"

# Each setting below either makes the test vacuous or fails it on a correct binary if it takes the
# wrong value, so all of them are pinned in the query itself. Most are randomized by
# tests/clickhouse-test, and a query-level SETTINGS clause wins over the runner's injection:
#   enable_filesystem_cache=1                                   -- 0 never builds the cache buffer
#   read_from_filesystem_cache_if_exists_otherwise_bypass_cache=0 -- 1 takes the bypass branch
#   use_uncompressed_cache=0                                    -- 1 routes through another class
#   max_read_buffer_size                                        -- small buffer => many refills
#   filesystem_cache_segments_batch_size=1                      -- makes the batch hold one segment,
#                                                                  so the per-refill resize log fires
#   min_bytes_to_use_direct_io / _mmap_io=0, local_filesystem_read_method='pread',
#   local_filesystem_read_prefetch=0, max_threads=1              -- keep the cached read path
#   enable_parallel_replicas=0                                  -- 1 sends the read to the replica
#                                                                  cluster, so this server's cache
#                                                                  buffer is not the one measured
# The rest are not randomized, but their defaults would widen the read buffer and so reduce the
# refill count the test depends on: max_read_buffer_size_local_fs,
# filesystem_cache_prefer_bigger_buffer_size.
read_settings="enable_filesystem_cache = 1, max_read_buffer_size = 4096,
    max_read_buffer_size_local_fs = 4096, use_uncompressed_cache = 0,
    read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0,
    min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0,
    local_filesystem_read_method = 'pread', local_filesystem_read_prefetch = 0,
    filesystem_cache_prefer_bigger_buffer_size = 0, enable_parallel_replicas = 0,
    filesystem_cache_segments_batch_size = 1, max_threads = 1"

# Warm the cache so the measured queries read from it (ReadType::CACHED) rather than downloading.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(cityHash64(s)), sum(k) FROM t_cached_read_log SETTINGS $read_settings
" > /dev/null

# send_logs_level=test makes the server evaluate the TEST-level logs (and hence the LogTest
# ProfileEvent) regardless of the server's own log level. The default-off arm scans the whole table,
# because only a large read count can show the absence of a storm; the verbose arm reads a small
# slice, because there the messages really are emitted and the point is only their per-read rate.
$CLICKHOUSE_CLIENT --send_logs_level=test --query_id="04825_${CLICKHOUSE_DATABASE}_0" -q "
    SELECT sum(cityHash64(s)), sum(k) FROM t_cached_read_log
    SETTINGS $read_settings, filesystem_cache_verbose_logging = 0
" > /dev/null 2>/dev/null

$CLICKHOUSE_CLIENT --send_logs_level=test --query_id="04825_${CLICKHOUSE_DATABASE}_1" -q "
    SELECT sum(cityHash64(s)), sum(k) FROM (SELECT k, s FROM t_cached_read_log LIMIT 8192)
    SETTINGS $read_settings, filesystem_cache_verbose_logging = 1
" > /dev/null 2>/dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Default (setting off): a handful of segment-level lines against thousands of cached reads. Setting
# on: the three per-refill lines are back, so the rate is about 3 messages per read - asserting the
# rate rather than `logs > reads` keeps the arm from passing when only one of the three sites fires.
# Both arms assert their own read count, so neither can pass by reading nothing from the cache.
$CLICKHOUSE_CLIENT -q "
    SELECT
        anyIf(reads, verbose = 0) > 1000 AS many_reads_by_default,
        anyIf(test_logs, verbose = 0) < 1000 AS few_test_logs_by_default,
        anyIf(reads, verbose = 1) > 50 AS many_reads_when_verbose,
        anyIf(test_logs, verbose = 1) / anyIf(reads, verbose = 1) > 2.5 AS verbose_logging_restores_them
    FROM
    (
        SELECT
            toUInt8(splitByChar('_', query_id)[-1]) AS verbose,
            ProfileEvents['CachedReadBufferReadFromCacheHits'] AS reads,
            ProfileEvents['LogTest'] AS test_logs
        FROM system.query_log
        WHERE query_id IN ('04825_${CLICKHOUSE_DATABASE}_0', '04825_${CLICKHOUSE_DATABASE}_1')
          AND current_database = currentDatabase() AND type = 'QueryFinish' AND read_rows > 0
        ORDER BY event_time_microseconds DESC
        LIMIT 1 BY verbose
    );
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cached_read_log;"
