-- Test Parquet metadata cache functionality
-- Tag: no-fasttest: Depends on AWS

DROP TABLE IF EXISTS test_03707_parquet_metadata_cache;

-- create a table with two parquet files using partitions
-- test_03707_parquet_metadata_cache_0 and test_03707_parquet_metadata_cache_1
CREATE TABLE test_03707_parquet_metadata_cache (
    id UInt32, 
    name String, 
    value Float64
) 
ENGINE = S3(s3_conn, filename='test_03707_parquet_metadata_cache_{_partition_id}', format='Parquet')
PARTITION BY id % 2;

INSERT INTO test_03707_parquet_metadata_cache VALUES (1, 'test1', 1.23), (2, 'test2', 4.56);

-- Test cache with S3 table function
-- Triggers caching of the file
SELECT 
    count() 
FROM s3(s3_conn, filename='test_03707_parquet_metadata_cache_*.parquet', format='Parquet');

-- Test cache with wildcards
-- Should be a cache hit
SELECT 
    count()
FROM s3(s3_conn, filename='test_03707_parquet_metadata_cache_*.parquet', format='Parquet');

SELECT
    sum(ProfileEvent_ParquetMetadataCacheMisses),
    sum(ProfileEvent_ParquetMetadataCacheHits),
    sum(ProfileEvent_ParquetMetadataCacheWeightLost)
FROM system.metric_log;

-- Another cache hit
SELECT
    count()
FROM s3(s3_conn, filename='test_03707_parquet_metadata_cache_*.parquet', format='Parquet');

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

DROP TABLE test_03707_parquet_metadata_cache;
-- TODO: Add tests for ParquetMetadataCacheBytes and ParquetMetadataCacheFiles
-- TODO: Add a test the triggers cache eviction
