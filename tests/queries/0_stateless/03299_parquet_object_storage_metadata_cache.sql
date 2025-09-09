-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS t_parquet_03262;

CREATE TABLE t_parquet_03262 (a UInt64)
ENGINE = S3(s3_conn, filename = 'test_03262_{_partition_id}', format = Parquet)
PARTITION BY a;

INSERT INTO t_parquet_03262 SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert=1;

SELECT COUNT(*)
FROM s3(s3_conn, filename = 'test_03262_*', format = Parquet)
SETTINGS input_format_parquet_use_metadata_cache=1, optimize_count_from_files=0;

SELECT COUNT(*)
FROM s3(s3_conn, filename = 'test_03262_*', format = Parquet)
SETTINGS input_format_parquet_use_metadata_cache=1, optimize_count_from_files=0, log_comment='test_03262_parquet_metadata_cache';

SELECT COUNT(*)
FROM s3(s3_conn, filename = 'test_03262_*', format = ParquetMetadata)
SETTINGS input_format_parquet_use_metadata_cache=1, log_comment='test_03262_parquet_metadata_format_metadata_cache';

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['ParquetMetaDataCacheHits']
FROM system.query_log
where log_comment = 'test_03262_parquet_metadata_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ParquetMetaDataCacheHits']
FROM system.query_log
where log_comment = 'test_03262_parquet_metadata_format_metadata_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SYSTEM DROP PARQUET METADATA CACHE;

SELECT COUNT(*)
FROM s3(s3_conn, filename = 'test_03262_*', format = Parquet)
SETTINGS input_format_parquet_use_metadata_cache=1, optimize_count_from_files=0, log_comment='test_03262_parquet_metadata_cache_cache_empty';

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['ParquetMetaDataCacheHits']
FROM system.query_log
where log_comment = 'test_03262_parquet_metadata_cache_cache_empty'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ParquetMetaDataCacheMisses']
FROM system.query_log
where log_comment = 'test_03262_parquet_metadata_cache_cache_empty'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

DROP TABLE t_parquet_03262;
