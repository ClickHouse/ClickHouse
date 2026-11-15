-- Tags: no-fasttest, no-parallel-replicas, no-random-settings
-- Tag no-fasttest: depends on MinIO via s3_conn.
-- Tag no-parallel-replicas: read_rows is reported per replica; assertion checks the local query_log.
-- Tag no-random-settings: depends on Parquet push-down defaults (filter_push_down, bloom_filter, page_index).

-- IN (subquery) on Parquet over object storage must push down to page index / row-group stats / bloom filter, same as a literal IN.

INSERT INTO FUNCTION s3(s3_conn,
    filename = '04267_s3_parquet_in_subquery/' || currentDatabase() || '.parquet',
    format = Parquet,
    structure = 'id UInt64, payload String')
SELECT number AS id, repeat('x', 16) AS payload
FROM numbers(200000)
ORDER BY id
SETTINGS
    output_format_parquet_row_group_size      = 20000,
    output_format_parquet_data_page_size      = 4096,
    output_format_parquet_write_page_index    = 1,
    output_format_parquet_write_bloom_filter  = 1,
    output_format_parquet_use_custom_encoder  = 1,
    max_threads                               = 1;

-- Three matching keys, deliberately in three different row groups.
SELECT count() FROM s3(s3_conn, filename = '04267_s3_parquet_in_subquery/' || currentDatabase() || '.parquet', format = Parquet)
WHERE id IN (100, 100000, 199900)
SETTINGS enable_filesystem_cache = 0, log_comment = '100743_s3_literal';

SELECT count() FROM s3(s3_conn, filename = '04267_s3_parquet_in_subquery/' || currentDatabase() || '.parquet', format = Parquet)
WHERE id IN (SELECT arrayJoin([100, 100000, 199900])::UInt64)
SETTINGS enable_filesystem_cache = 0, log_comment = '100743_s3_subquery';

WITH keys AS (SELECT arrayJoin([100::UInt64, 100000::UInt64, 199900::UInt64]) AS id)
SELECT count() FROM s3(s3_conn, filename = '04267_s3_parquet_in_subquery/' || currentDatabase() || '.parquet', format = Parquet)
WHERE id IN (SELECT id FROM keys)
SETTINGS enable_filesystem_cache = 0, log_comment = '100743_s3_cte';

-- Multiple independent IN (subquery) nodes ORed together; each ColumnSet must be materialised.
SELECT count() FROM s3(s3_conn, filename = '04267_s3_parquet_in_subquery/' || currentDatabase() || '.parquet', format = Parquet)
WHERE id IN (SELECT arrayJoin([100])::UInt64)
   OR id IN (SELECT arrayJoin([100000])::UInt64)
   OR id IN (SELECT arrayJoin([199900])::UInt64)
SETTINGS enable_filesystem_cache = 0, log_comment = '100743_s3_or_subqueries';

-- For each SELECT the expectation is to read far less than the full file (200k rows) - 50k as threshold should be enough
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    read_rows < 50000 AS pushed_down
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment IN ('100743_s3_literal', '100743_s3_subquery', '100743_s3_cte', '100743_s3_or_subqueries')
ORDER BY log_comment;
