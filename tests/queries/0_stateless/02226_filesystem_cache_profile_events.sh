#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-random-settings, no-replicated-database, no-parallel:filesystem-cache
# Tag no-parallel: cache hit and miss assertions must not overlap tests that
# clear or heavily mutate the process-wide filesystem-cache registry.

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for STORAGE_POLICY in 's3_cache' 'local_cache' 'azure_cache'; do
    echo "Using storage policy: $STORAGE_POLICY"

    # Use a per-test inline cache disk over the same backing store the named
    # policy would use. The shared CI caches (s3_cache is only 200 MiB) are
    # thrashed by concurrent tests, which both evicts this test's segments
    # between queries and starves cache-write reservations - the hit/miss
    # assertions below need a cache nothing else touches.
    case "$STORAGE_POLICY" in
        s3_cache)    UNDERLYING_DISK="s3_disk" ;;
        local_cache) UNDERLYING_DISK="local_disk" ;;
        azure_cache) UNDERLYING_DISK="azure" ;;
    esac
    CACHE_NAME="02226_${STORAGE_POLICY}"

    $CLICKHOUSE_CLIENT --multiline  --query """
    SET max_memory_usage='20G';
    SET enable_filesystem_cache_on_write_operations = 0;

    DROP TABLE IF EXISTS test_02226;
    CREATE TABLE test_02226 (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS disk = disk(type = cache, name = '$CACHE_NAME', path = '$CACHE_NAME/', max_size = '100Mi', cache_on_write_operations = 1, disk = '$UNDERLYING_DISK');
    INSERT INTO test_02226 SELECT * FROM generateRandom('key UInt32, value String') LIMIT 10000;

    SET remote_filesystem_read_method='threadpool';
    """

    query="SELECT * FROM test_02226 LIMIT 10"

    query_id_1=$($CLICKHOUSE_CLIENT --query "select queryID() from ($query) limit 1" 2>&1)

    $CLICKHOUSE_CLIENT --multiline --query """
    set remote_filesystem_read_method = 'read';
    set local_filesystem_read_method = 'pread';
    """

    query_id_2=$($CLICKHOUSE_CLIENT --query "select queryID() from ($query) limit 1" 2>&1)


    $CLICKHOUSE_CLIENT --multiline --query """
    set remote_filesystem_read_method='threadpool';
    """

    query_id_3=$($CLICKHOUSE_CLIENT --query "select queryID() from ($query) limit 1")

    # The three profiled queries run back-to-back and the query_log checks happen
    # afterwards: the checks assert filesystem-cache hits, and an intervening flush
    # plus query_log scan gives concurrent tests' cache traffic a seconds-long
    # window to evict this test's cached segments (the shared cache is small).
    $CLICKHOUSE_CLIENT --multiline  --query """
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['CachedReadBufferReadFromCacheHits'] > 0 as remote_fs_cache_hit,
           ProfileEvents['CachedReadBufferReadFromCacheMisses'] > 0 as remote_fs_cache_miss,
           ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
           ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
           ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id='$query_id_1'
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    ORDER BY query_start_time DESC
    LIMIT 1;
    """

    $CLICKHOUSE_CLIENT --multiline  --query """
    SELECT ProfileEvents['CachedReadBufferReadFromCacheHits'] > 0 as remote_fs_cache_hit,
           ProfileEvents['CachedReadBufferReadFromCacheMisses'] > 0 as remote_fs_cache_miss,
           ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
           ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
           ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id='$query_id_2'
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    ORDER BY query_start_time DESC
    LIMIT 1;
    """

    $CLICKHOUSE_CLIENT --multiline  --query """
    SELECT ProfileEvents['CachedReadBufferReadFromCacheHits'] > 0 as remote_fs_cache_hit,
           ProfileEvents['CachedReadBufferReadFromCacheMisses'] > 0 as remote_fs_cache_miss,
           ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
           ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
           ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id='$query_id_3'
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    ORDER BY query_start_time DESC
    LIMIT 1;
    """


    $CLICKHOUSE_CLIENT --multiline  --query """
    SELECT * FROM test_02226 WHERE value LIKE '%abc%' ORDER BY value LIMIT 10 FORMAT Null;

    SET enable_filesystem_cache_on_write_operations = 1;

    TRUNCATE TABLE test_02226;
    SELECT count() FROM test_02226;

    SYSTEM CLEAR FILESYSTEM CACHE '$CACHE_NAME';

    INSERT INTO test_02226 SELECT * FROM generateRandom('key UInt32, value String') LIMIT 10000;
    """

    $CLICKHOUSE_CLIENT --query "DROP TABLE test_02226"
done
