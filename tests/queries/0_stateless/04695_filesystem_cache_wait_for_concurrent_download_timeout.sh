#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-replicated-database, no-object-storage, no-parallel-replicas
# no-parallel: enables a global pauseable failpoint which pauses every filesystem cache write on the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A private cache keeps the test independent from other cache users on the server (e.g. background
# merges of `test.hits_s3` write through the shared `s3_cache` and can fill it with non-releasable
# segments, failing this table's space reservations). The whole data file must fit in one file
# segment, so that the waiter below never becomes a downloader itself (it would pause on its own
# cache write and deadlock, because the failpoint is disabled only after it returns).
cache_name="cache_04695_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_download_wait_timeout;
    CREATE TABLE t_download_wait_timeout (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(
        type = cache,
        name = '$cache_name',
        path = '$cache_name/',
        max_size = '1Gi',
        max_file_segment_size = '256Mi',
        cache_on_write_operations = 0,
        load_metadata_asynchronously = 0,
        disk = 'local_disk');
    INSERT INTO t_download_wait_timeout SELECT number FROM numbers(1000000);
"

# Warm up the in-memory mark cache, so that both queries below take marks from it and their only
# filesystem cache accesses are for the data file. Otherwise the paused downloader would hold the
# mark cache load token and the waiter would block on it instead of `FileSegment::wait`.
# Strictly synchronous, no prefetches and no background download: a cache write still in flight
# after this query would pause on the failpoint while holding a segment the queries below need.
$CLICKHOUSE_CLIENT --max_threads 1 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --filesystem_cache_allow_background_download 0 \
    --query "SELECT a FROM t_download_wait_timeout LIMIT 1 FORMAT Null"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP FILESYSTEM CACHE '$cache_name';
    SYSTEM ENABLE FAILPOINT file_segment_pause_before_write;
"

# The failpoint is server-global, so it must be disabled even if the script aborts before reaching
# the explicit disable below. Otherwise every filesystem cache write on the server stays paused and
# the following tests hang behind it, masking the failure that happened here.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT file_segment_pause_before_write" > /dev/null 2>&1 || true' EXIT

# The downloader pauses inside its first cache write, holding the file segment in DOWNLOADING state.
$CLICKHOUSE_CLIENT --max_threads 1 \
    --enable_filesystem_cache 1 \
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --filesystem_cache_allow_background_download 0 \
    --query "SELECT sum(a) FROM t_download_wait_timeout" &

# The failpoint pauses every cache write on the server, so the first pause is not necessarily the
# downloader's: additionally wait until the downloader holds the segment of this test's cache.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT file_segment_pause_before_write PAUSE"
downloading=0
for _ in {1..600}; do
    downloading=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name' AND state = 'DOWNLOADING'")
    [[ "$downloading" == "1" ]] && break
    sleep 0.1
done
echo "$downloading"

# The waiter needs the paused segment, gives up waiting after 100 ms and bypasses the cache.
waiter_query_id="04695_waiter_${CLICKHOUSE_DATABASE}_${RANDOM}"
$CLICKHOUSE_CLIENT --query_id "$waiter_query_id" --max_threads 1 \
    --enable_filesystem_cache 1 \
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 \
    --remote_filesystem_read_prefetch 0 \
    --allow_prefetched_read_pool_for_remote_filesystem 0 \
    --filesystem_cache_allow_background_download 0 \
    --use_uncompressed_cache 0 \
    --filesystem_cache_wait_for_concurrent_download_timeout_milliseconds 100 \
    --query "SELECT sum(a) FROM t_download_wait_timeout"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT file_segment_pause_before_write"
wait

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['FileSegmentWaitTimeouts'] >= 1
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$waiter_query_id' AND type = 'QueryFinish'
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_download_wait_timeout"
