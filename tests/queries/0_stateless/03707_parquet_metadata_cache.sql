-- Test Parquet metadata cache functionality
-- Tag: no-fasttest: Depends on AWS
SET log_queries = 1;
SYSTEM DROP PARQUET METADATA CACHE;

-- Test cache with S3 table function
-- Triggers caching of the file
SELECT
    *
FROM s3(s3_conn, filename='03707_cache_test.parquet', format='Parquet') 
settings log_comment = '03707-first-test-query';

-- should be a cache miss as we load the cache the first time
SELECT
    ProfileEvents['ParquetMetadataCacheHits'],
    ProfileEvents['ParquetMetadataCacheMisses'],
FROM system.query_log
WHERE log_comment = '03707-first-test-query'
AND type = 'QueryFinish';

-- Should be a cache hit as we use the same file
SELECT
    id, upper(name), value + 1
FROM s3(s3_conn, filename='03707_cache_test.parquet', format='Parquet')
settings log_comment = '03707-second-test-query';

SELECT
    ProfileEvents['ParquetMetadataCacheHits'],
    ProfileEvents['ParquetMetadataCacheMisses'],
FROM system.query_log
WHERE log_comment = '03707-second-test-query'
AND type = 'QueryFinish';

SYSTEM DROP PARQUET METADATA CACHE;
