#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-replicated-database, no-object-storage, no-parallel-replicas
# no-parallel, no-object-storage: uses a one-time global failpoint which must be consumed
# by the query below and not by an unrelated concurrent write through a filesystem cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_download_wait_timeout;
    CREATE TABLE t_download_wait_timeout (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 's3_cache';
    INSERT INTO t_download_wait_timeout SELECT number FROM numbers(1000000) SETTINGS enable_filesystem_cache_on_write_operations = 0;
"

# Warm up the in-memory mark cache, so that both queries below take marks from it and their only
# filesystem cache accesses are for the data file. Otherwise the paused downloader would hold the
# mark cache load token and the waiter would block on it instead of `FileSegment::wait`.
$CLICKHOUSE_CLIENT --filesystem_cache_allow_background_download 0 \
    --query "SELECT a FROM t_download_wait_timeout LIMIT 1 FORMAT Null"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP FILESYSTEM CACHE;
    SYSTEM ENABLE FAILPOINT file_segment_pause_before_write;
"

# The downloader pauses inside its first cache write, holding that file segment in DOWNLOADING state.
$CLICKHOUSE_CLIENT --max_threads 1 \
    --enable_filesystem_cache 1 \
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --query "SELECT sum(a) FROM t_download_wait_timeout" &

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT file_segment_pause_before_write PAUSE"

# The waiter needs the paused segment, gives up waiting after 100 ms and bypasses the cache.
waiter_query_id="waiter_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$waiter_query_id" --max_threads 1 \
    --enable_filesystem_cache 1 \
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --use_uncompressed_cache 0 \
    --filesystem_cache_wait_for_concurrent_download_timeout_milliseconds 100 \
    --query "SELECT sum(a) FROM t_download_wait_timeout"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT file_segment_pause_before_write"
wait

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['CachedReadBufferDownloadWaitTimeouts'] >= 1
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$waiter_query_id' AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_download_wait_timeout"
