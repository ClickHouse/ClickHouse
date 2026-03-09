-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: depends on s3 storage
-- no-parallel: cache is system-wide and tests can affect each other in unexpected way
-- no-parallel-replicas: profile events are not available on the second replica

/*
Because the parquet metadata cache is system-wide, parallel runs of 
SYSTEM DROP PARQUET METADATA CACHE will lead to non-deterministic results
*/
SET log_queries = 1;
SYSTEM DROP PARQUET METADATA CACHE;

-- Triggers caching of the file
-- should be a cache miss as we load the cache the first time
SELECT *
FROM s3(s3_conn, filename = '03707_cache_test.parquet', format = 'Parquet')
SETTINGS log_comment = '03707-first-test-query', use_parquet_metadata_cache = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses
FROM system.query_log
WHERE (log_comment = '03707-first-test-query') AND (type = 'QueryFinish') AND (current_database = currentDatabase());

-- Should be a cache hit as we use the same file
SELECT
    id,
    upper(name),
    value + 1
FROM s3(s3_conn, filename = '03707_cache_test.parquet', format = 'Parquet')
SETTINGS log_comment = '03707-second-test-query', use_parquet_metadata_cache = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses
FROM system.query_log
WHERE (log_comment = '03707-second-test-query') AND (type = 'QueryFinish') AND (current_database = currentDatabase());

SYSTEM DROP PARQUET METADATA CACHE;

-- Should be back to a cache miss since we dropped the cache
SELECT
    name,
    value + value
FROM s3(s3_conn, filename = '03707_cache_test.parquet', format = 'Parquet')
SETTINGS log_comment = '03707-third-test-query', use_parquet_metadata_cache = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses
FROM system.query_log
WHERE (log_comment = '03707-third-test-query') AND (type = 'QueryFinish') AND (current_database = currentDatabase());

