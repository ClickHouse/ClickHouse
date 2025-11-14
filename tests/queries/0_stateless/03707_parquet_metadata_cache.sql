-- Test Parquet metadata cache functionality
-- Tags: no-fasttest: Depends on AWS

CREATE TABLE test_parquet_metadata_cache (
    id UInt32, 
    name String, 
    value Float64
) 
ENGINE = S3(s3_conn, filename='test_cache_{_partition_id}', format='Parquet') 
PARTITION BY id % 2;

INSERT INTO test_parquet_metadata_cache VALUES (1, 'test1', 1.23), (2, 'test2', 4.56);

-- Test cache with S3 table function
-- Triggers caching of the file
SELECT 
    count() 
FROM s3(s3_conn, filename='test_cache_*', format='Parquet');

-- Test cache with wildcards
-- Should be a cache hit
SELECT 
    * 
FROM s3(s3_conn, filename='test_cache_*.parquet', format='Parquet');

SELECT
    sum(ProfileEvent_ParquetMetadataCacheMisses),
    sum(ProfileEvent_ParquetMetadataCacheHits),
    sum(ProfileEvent_ParquetMetadataCacheWeightLost)
FROM system.metric_log;

-- Another cache hit
SELECT
    *
FROM s3(s3_conn, filename='test_cache_*.parquet', format='Parquet');

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