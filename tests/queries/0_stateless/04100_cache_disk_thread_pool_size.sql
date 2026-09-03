-- Tags: no-fasttest, no-replicated-database

-- Verify that thread_pool_size is accepted on cache-wrapped disks.
-- Previously, FileCacheSettings::loadFromConfig() would reject
-- thread_pool_size as UNKNOWN_SETTING, crashing the server on startup.

DROP TABLE IF EXISTS test_cache_thread_pool;
CREATE TABLE test_cache_thread_pool (key UInt32, value String)
Engine=MergeTree()
ORDER BY key
SETTINGS disk = disk(
    type = cache,
    name = '04100_cache_disk_thread_pool_size',
    max_size = '16Mi',
    path = 'cache_thread_pool_size_test/',
    thread_pool_size = 32,
    disk = 's3_disk');

INSERT INTO test_cache_thread_pool SELECT number, toString(number) FROM numbers(10);
SELECT count() FROM test_cache_thread_pool;
DROP TABLE test_cache_thread_pool;
