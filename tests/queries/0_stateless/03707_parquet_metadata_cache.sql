-- Test Parquet metadata cache functionality
-- Tag: no-fasttest: Depends on AWS
SET log_queries = 1;
SYSTEM DROP PARQUET METADATA CACHE;
DELETE FROM system.query_log WHERE log_comment like '03707%';

-- Triggers caching of the file
-- should be a cache miss as we load the cache the first time
SELECT *
FROM s3(s3_conn, filename = '03707_cache_test.parquet', format = 'Parquet')
SETTINGS log_comment = '03707-first-test-query';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses
FROM system.query_log
WHERE (log_comment = '03707-first-test-query') AND (type = 'QueryFinish');

-- Should be a cache hit as we use the same file
SELECT
    id,
    upper(name),
    value + 1
FROM s3(s3_conn, filename = '03707_cache_test.parquet', format = 'Parquet')
SETTINGS log_comment = '03707-second-test-query';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses
FROM system.query_log
WHERE (log_comment = '03707-second-test-query') AND (type = 'QueryFinish');

SYSTEM DROP PARQUET METADATA CACHE;

-- Should be back to a cache miss since we dropped the cache
SELECT
    name,
    value + value
FROM s3(s3_conn, filename = '03707_cache_test.parquet', format = 'Parquet')
SETTINGS log_comment = '03707-third-test-query';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses
FROM system.query_log
WHERE (log_comment = '03707-third-test-query') AND (type = 'QueryFinish');

