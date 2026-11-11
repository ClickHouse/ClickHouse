-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: depends on S3
-- no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
-- no-parallel-replicas: profile events are not available on the second replica
-- no-random-settings: which columns are cached depends on the cache settings

-- The columns cache for `Parquet` files read from an object storage. The ETag of the object
-- identifies its version.

SET log_queries = 1;
SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = '05339_columns_cache_' || currentDatabase() || '.parquet', format = Parquet)
SELECT number AS id, toString(number) AS s, [number, number + 1] AS arr
FROM numbers(5000)
SETTINGS output_format_parquet_row_group_size = 1000;

SYSTEM DROP COLUMNS CACHE;

SELECT sum(id), sum(length(s)), sum(arraySum(arr))
FROM s3(s3_conn, filename = '05339_columns_cache_' || currentDatabase() || '.parquet', format = Parquet)
SETTINGS use_columns_cache = 1, log_comment = '05339-first';

SELECT sum(id), sum(length(s)), sum(arraySum(arr))
FROM s3(s3_conn, filename = '05339_columns_cache_' || currentDatabase() || '.parquet', format = Parquet)
SETTINGS use_columns_cache = 1, log_comment = '05339-second';

SELECT column, count(), sum(rows)
FROM system.columns_cache
WHERE endsWith(part, '05339_columns_cache_' || currentDatabase() || '.parquet')
GROUP BY column ORDER BY column;

-- The object is overwritten: it gets another ETag, so the cached columns are not used.
INSERT INTO FUNCTION s3(s3_conn, filename = '05339_columns_cache_' || currentDatabase() || '.parquet', format = Parquet)
SELECT number * 2 AS id, toString(number * 2) AS s, [number, number * 2] AS arr
FROM numbers(5000)
SETTINGS output_format_parquet_row_group_size = 1000;

SELECT sum(id), sum(length(s)), sum(arraySum(arr))
FROM s3(s3_conn, filename = '05339_columns_cache_' || currentDatabase() || '.parquet', format = Parquet)
SETTINGS use_columns_cache = 1, log_comment = '05339-rewritten';

SELECT sum(id), sum(length(s)), sum(arraySum(arr))
FROM s3(s3_conn, filename = '05339_columns_cache_' || currentDatabase() || '.parquet', format = Parquet)
SETTINGS use_columns_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ColumnsCacheHits'] AS hits, ProfileEvents['ColumnsCacheMisses'] AS misses
FROM system.query_log
WHERE log_comment LIKE '05339-%' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- Writes disabled: nothing is written.
SYSTEM DROP COLUMNS CACHE;

SELECT sum(id)
FROM s3(s3_conn, filename = '05339_columns_cache_' || currentDatabase() || '.parquet', format = Parquet)
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 0;

SELECT count()
FROM system.columns_cache
WHERE endsWith(part, '05339_columns_cache_' || currentDatabase() || '.parquet');
