#!/usr/bin/env bash
# Tags: stateful, no-flaky-check, no-parallel, no-random-settings, long, no-asan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# The queries below require the whole table to be in the cache, so it needs a cache large enough to
# hold it and private to this test: on the shared `s3_cache` another user can hold every segment
# non-releasable, and a reservation that then finds nothing to evict silently reads past the cache.
# `test.hits_s3` is too large, hence the 1% sample.
cache_name="cache_00180_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hits_s3_sampled"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE hits_s3_sampled AS test.hits_s3
    ENGINE = MergeTree
    SETTINGS disk = disk(
        type = cache,
        name = '$cache_name',
        path = '$cache_name/',
        max_size = '1Gi',
        max_file_segment_size = '5Mi',
        cache_on_write_operations = 1,
        load_metadata_asynchronously = 0,
        disk = 's3_disk')"
$CLICKHOUSE_CLIENT -q "INSERT INTO hits_s3_sampled SELECT * FROM test.hits_s3 SAMPLE 0.01"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE hits_s3_sampled FINAL"

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE '$cache_name'"

# Warm up the cache
$CLICKHOUSE_CLIENT -q "SELECT * FROM hits_s3_sampled WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS filesystem_cache_reserve_space_wait_lock_timeout_milliseconds=2000"
$CLICKHOUSE_CLIENT -q "SELECT * FROM hits_s3_sampled WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS filesystem_cache_reserve_space_wait_lock_timeout_milliseconds=2000"

query_id=02906_read_from_cache_$RANDOM
$CLICKHOUSE_CLIENT --query_id ${query_id} -q "SELECT * FROM hits_s3_sampled WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS filesystem_cache_reserve_space_wait_lock_timeout_milliseconds=2000"

$CLICKHOUSE_CLIENT -q "
  SYSTEM FLUSH LOGS query_log;

  -- AsynchronousReaderIgnoredBytes = 0: no seek-avoiding happened
  -- CachedReadBufferReadFromSourceBytes = 0: sanity check to ensure we read only from cache
  SELECT ProfileEvents['AsynchronousReaderIgnoredBytes'], ProfileEvents['CachedReadBufferReadFromSourceBytes']
  FROM system.query_log
  WHERE query_id = '$query_id' AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS hits_s3_sampled"
