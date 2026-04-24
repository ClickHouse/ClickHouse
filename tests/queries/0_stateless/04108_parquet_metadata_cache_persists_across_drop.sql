-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: depends on s3 storage
-- no-parallel: cache is system-wide and tests can affect each other in unexpected ways
-- no-parallel-replicas: profile events are not available on the second replica
-- no-random-settings: we need to test the interaction of specific setting combinations

-- The parquet metadata cache is keyed by (file_path, etag) at the server level, not bound to any
-- IStorage object. Dropping a table that referenced an S3 parquet file and recreating a new table
-- over the same file must reuse the cached metadata rather than re-parse the footer.

SET log_queries = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = '04108_parquet_cache_persists_across_drop.parquet', format = 'Parquet')
SELECT number AS id, toString(number) AS name FROM numbers(100)
SETTINGS s3_truncate_on_insert = 1;

SYSTEM DROP PARQUET METADATA CACHE;

DROP TABLE IF EXISTS t_parquet_cache_1;
DROP TABLE IF EXISTS t_parquet_cache_2;

CREATE TABLE t_parquet_cache_1 (id UInt64, name String)
    ENGINE = S3(s3_conn, filename = '04108_parquet_cache_persists_across_drop.parquet', format = 'Parquet');

-- q1: fresh cache -> expect miss
SELECT * FROM t_parquet_cache_1
    SETTINGS log_comment = '04108-q1', use_parquet_metadata_cache = 1, input_format_parquet_use_native_reader_v3 = 1
    FORMAT Null;

-- q2: same table, same file -> expect hit (sanity: caching works at all)
SELECT * FROM t_parquet_cache_1
    SETTINGS log_comment = '04108-q2', use_parquet_metadata_cache = 1, input_format_parquet_use_native_reader_v3 = 1
    FORMAT Null;

DROP TABLE t_parquet_cache_1;

-- q3: recreate a different table over the same file -> expect hit (the payoff)
CREATE TABLE t_parquet_cache_2 (id UInt64, name String)
    ENGINE = S3(s3_conn, filename = '04108_parquet_cache_persists_across_drop.parquet', format = 'Parquet');
SELECT * FROM t_parquet_cache_2
    SETTINGS log_comment = '04108-q3', use_parquet_metadata_cache = 1, input_format_parquet_use_native_reader_v3 = 1
    FORMAT Null;

-- q4: drop the cache and re-query -> expect miss (confirms the cache is the causal factor)
SYSTEM DROP PARQUET METADATA CACHE;
SELECT * FROM t_parquet_cache_2
    SETTINGS log_comment = '04108-q4', use_parquet_metadata_cache = 1, input_format_parquet_use_native_reader_v3 = 1
    FORMAT Null;

DROP TABLE t_parquet_cache_2;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04108-q1';

SELECT ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04108-q2';

SELECT ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04108-q3';

SELECT ProfileEvents['ParquetMetadataCacheHits'], ProfileEvents['ParquetMetadataCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04108-q4';
