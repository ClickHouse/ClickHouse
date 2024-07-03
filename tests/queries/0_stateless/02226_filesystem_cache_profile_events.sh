#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-s3-storage, no-random-settings, no-cpu-aarch64, no-replicated-database

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


for STORAGE_POLICY in 's3_cache' 'local_cache' 'azure_cache'; do
    echo "Using storage policy: $STORAGE_POLICY"

    clickhouse client --multiquery --multiline  --query """
    SET max_memory_usage='20G';
    SET enable_filesystem_cache_on_write_operations = 0;

    DROP TABLE IF EXISTS test_02226;
    CREATE TABLE test_02226 (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='$STORAGE_POLICY';
    INSERT INTO test_02226 SELECT * FROM generateRandom('key UInt32, value String') LIMIT 10000;

    SET remote_filesystem_read_method='threadpool';
    """

    query="SELECT * FROM test_02226 LIMIT 10"

    query_id=$(clickhouse client --query "select queryID() from ($query) limit 1" 2>&1)

    clickhouse client --multiquery --multiline  --query """
    SYSTEM FLUSH LOGS;
    SELECT ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
           ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
           ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
    FROM system.query_log
    WHERE query_id='$query_id'
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    ORDER BY query_start_time DESC
    LIMIT 1;
    """

    clickhouse client --multiquery --multiline --query """
    set remote_filesystem_read_method = 'read';
    set local_filesystem_read_method = 'pread';
    """

    query_id=$(clickhouse client --query "select queryID() from ($query) limit 1" 2>&1)

    clickhouse client --multiquery --multiline  --query """
    SYSTEM FLUSH LOGS;
    SELECT ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
           ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
           ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
    FROM system.query_log
    WHERE query_id='$query_id'
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    ORDER BY query_start_time DESC
    LIMIT 1;
    """


    clickhouse client --multiquery --multiline --query """
    set remote_filesystem_read_method='threadpool';
    """

    query_id=$(clickhouse client --query "select queryID() from ($query) limit 1")

    clickhouse client --multiquery --multiline  --query """
    SYSTEM FLUSH LOGS;
    SELECT ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
           ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
           ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
    FROM system.query_log
    WHERE query_id='$query_id'
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    ORDER BY query_start_time DESC
    LIMIT 1;
    """

    clickhouse client --multiquery --multiline  --query """
    SELECT * FROM test_02226 WHERE value LIKE '%abc%' ORDER BY value LIMIT 10 FORMAT Null;

    SET enable_filesystem_cache_on_write_operations = 1;

    TRUNCATE TABLE test_02226;
    SELECT count() FROM test_02226;

    SYSTEM DROP FILESYSTEM CACHE;

    INSERT INTO test_02226 SELECT * FROM generateRandom('key UInt32, value String') LIMIT 10000;
    """

    clickhouse client --query "DROP TABLE test_02226"
done
