-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: depends on S3, Parquet and Iceberg
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- A filter or a row policy on a column that is added after the data file is read (a Hive partition column,
-- a virtual column) uses the value of that column, even when the data file has a column with the same name.

-- The path says `key = 9`, the data files store `key = 10`, NULL and `_file = 'evil'`.
INSERT INTO FUNCTION file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') SELECT 10, number FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05325/nullable/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') SELECT NULL, number FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05325/virtual/data.parquet', Parquet, '_file String, v Int64') SELECT 'evil', number FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05325/count/key=9/data.parquet', Parquet, 'key Int64, v Int64') SELECT 10, number FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05325/join/key=9/data.parquet', Parquet, 'v Int64') SELECT number AS v FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05325/topk/key=9/data.parquet', Parquet, 'v Int64') SELECT number AS v FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05325/only/a=1/key=9/data.parquet', Parquet, 'key Int64') SELECT 10 FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05325/key=9/data.parquet', format = Parquet, structure = 'key Int64, v Int64') SELECT 10, number FROM numbers(1000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

-- The Iceberg data file is written before `RENAME COLUMN _path TO old_path`, so it stores a column `_path`.
SET allow_insert_into_iceberg = 1;
CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05325/iceberg/') AS path
FROM system.server_settings WHERE name = 'user_files_path';
CREATE TABLE t_05325_iceberg (_path String, v Int64) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');
INSERT INTO t_05325_iceberg SELECT 'evil', number FROM numbers(1000) SETTINGS output_format_parquet_row_group_size = 100;
ALTER TABLE t_05325_iceberg RENAME COLUMN _path TO old_path;

-- The row count cache ignores an entry registered in the same second the file was written.
SELECT sleep(1) FORMAT Null;

SET use_hive_partitioning = 1, optimize_count_from_files = 0;
SET input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 1048576;

-- { echoOn }
SELECT key, count() FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') GROUP BY key;
SELECT count() FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key = 9;
SELECT count() FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key = 10;
SELECT count() FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key = 9 AND v < 10;
SELECT count() FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key = 9 OR v < 10;
SELECT count() FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key IN (9);
SELECT count(), sum(v) FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet) WHERE key = 9;
SELECT count() FROM file(currentDatabase() || '/05325/nullable/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') WHERE key IS NOT NULL;
SELECT count() FROM file(currentDatabase() || '/05325/only/a=1/key=9/data.parquet', Parquet, 'key Int64') WHERE indexHint(key = 9);

CREATE TABLE t_05325_s3 (key Int64, v Int64) ENGINE = S3(s3_conn, filename = currentDatabase() || '/05325/key=9/data.parquet', format = Parquet);
SELECT count() FROM t_05325_s3 WHERE key = 9 AND v < 10 SETTINGS use_query_condition_cache = 1, log_comment = '05325_qcc_1';
SELECT count() FROM t_05325_s3 WHERE key = 9 AND v < 10 SETTINGS use_query_condition_cache = 1, log_comment = '05325_qcc_2';
DROP TABLE t_05325_s3;
SELECT count() FROM url('http://localhost:11111/test/' || currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key = 9;

SELECT _file, count() FROM file(currentDatabase() || '/05325/virtual/data.parquet', Parquet, 'v Int64') GROUP BY _file;
SELECT count() FROM file(currentDatabase() || '/05325/virtual/data.parquet', Parquet, 'v Int64') WHERE _file = 'data.parquet';

CREATE TABLE t_05325_file AS file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64');
CREATE ROW POLICY p_05325_file ON t_05325_file USING key = 9 TO ALL;
SELECT count() FROM t_05325_file;
DROP ROW POLICY p_05325_file ON t_05325_file;
DROP TABLE t_05325_file;

SELECT count(), any(old_path) FROM t_05325_iceberg;
SELECT count() FROM t_05325_iceberg WHERE _path != 'evil';
DROP TABLE t_05325_iceberg;

-- The format still prunes row groups on the columns of the data file.
SELECT count() FROM file(currentDatabase() || '/05325/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key = 9 AND v < 10 SETTINGS log_comment = '05325_prune';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParquetPrunedRowGroups'] > 0 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05325_prune' AND type = 'QueryFinish';
SELECT sum(ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses']) > 0 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05325_qcc_2' AND type = 'QueryFinish';

-- A PREWHERE applied by the format does not let the source cache the filtered row count as the file's row count.
SELECT count(), sum(v) FROM file(currentDatabase() || '/05325/count/key=9/data.parquet', Parquet, 'key Int64, v Int64') PREWHERE v < 100 + 0 * rand() WHERE key = 9;
SELECT count() FROM file(currentDatabase() || '/05325/count/key=9/data.parquet', Parquet, 'key Int64, v Int64') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1, optimize_trivial_count_query = 1;
SELECT count() FROM file(currentDatabase() || '/05325/count/key=9/data.parquet', Parquet, 'key Int64, v Int64') PREWHERE v < 100 + 0 * rand() WHERE key = 9;

-- Neither does a join runtime filter nor the top-K filter applied by the format.
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM file(currentDatabase() || '/05325/join/key=9/data.parquet', Parquet, 'key Int64, v Int64') AS f INNER JOIN (SELECT toInt64(number) AS v FROM numbers(10)) AS t ON f.v = t.v WHERE f.key = 9 SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash,parallel_hash', query_plan_join_swap_table = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, query_plan_max_step_description_length = 1000) WHERE explain ILIKE '%prewhere filter column: %__applyfilter(%';
SELECT count() FROM file(currentDatabase() || '/05325/join/key=9/data.parquet', Parquet, 'key Int64, v Int64') AS f INNER JOIN (SELECT toInt64(number) AS v FROM numbers(10)) AS t ON f.v = t.v WHERE f.key = 9 SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash,parallel_hash', query_plan_join_swap_table = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT count() FROM file(currentDatabase() || '/05325/join/key=9/data.parquet', Parquet, 'key Int64, v Int64') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1, optimize_trivial_count_query = 1;
SELECT v FROM file(currentDatabase() || '/05325/topk/key=9/data.parquet', Parquet, 'key Int64, v Int64') WHERE key = 9 ORDER BY v LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1, max_threads = 1, max_parsing_threads = 1;
SELECT count() FROM file(currentDatabase() || '/05325/topk/key=9/data.parquet', Parquet, 'key Int64, v Int64') SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1, optimize_trivial_count_query = 1;
