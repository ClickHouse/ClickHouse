-- Test Parquet metadata cache functionality
-- Tag: no-fasttest: Depends on AWS

SYSTEM DROP PARQUET METADATA CACHE;

-- Test cache with S3 table function
-- Triggers caching of the file
SELECT
    count()
FROM s3(s3_conn, filename='03707_cache_test.parquet', format='Parquet');

-- Test cache with wildcards
-- Should be a cache hit
SELECT
    count()
FROM s3(s3_conn, filename='03707_cache_test.parquet', format='Parquet');

SELECT
    sum(ProfileEvent_ParquetMetadataCacheMisses),
    sum(ProfileEvent_ParquetMetadataCacheHits),
    sum(ProfileEvent_ParquetMetadataCacheWeightLost)
FROM system.metric_log;

-- Another cache hit
SELECT
    count()
FROM s3(s3_conn, filename='03707_cache_test.parquet', format='Parquet');

SELECT
    sum(ProfileEvent_ParquetMetadataCacheMisses),
    sum(ProfileEvent_ParquetMetadataCacheHits),
    sum(ProfileEvent_ParquetMetadataCacheWeightLost)
FROM system.metric_log;

/*
SELECT
    sum(CurrentMetric_ParquetMetadataCacheBytes),
    sum(CurrentMetric_ParquetMetadataCacheFiles)
FROM system.metric_log;
*/

SYSTEM DROP PARQUET METADATA CACHE;

-- Should be all zero
SELECT
    sum(ProfileEvent_ParquetMetadataCacheMisses),
    sum(ProfileEvent_ParquetMetadataCacheHits),
    sum(ProfileEvent_ParquetMetadataCacheWeightLost)
FROM system.metric_log;

-- TODO: Add tests for ParquetMetadataCacheBytes and ParquetMetadataCacheFiles
-- TODO: Add a test the triggers cache eviction

