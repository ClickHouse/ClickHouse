-- { echo }
-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3

-- Reading only hive partition columns or virtual columns must not lose rows when a hive partition column is the smallest column.

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05316/key=9/data.jsonl', format = JSONEachRow) SETTINGS s3_truncate_on_insert = 1 SELECT 'a' AS s;
INSERT INTO FUNCTION file(currentDatabase() || '/05316/key=9/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 'a' AS s;

-- A cached row count is used only for a file last modified before the second it was cached in.
SELECT sleep(1) FORMAT Null;

SET use_hive_partitioning = 1;

SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05316/key=9/data.jsonl', format = JSONEachRow) SETTINGS optimize_count_from_files = 0, use_cache_for_count_from_files = 1;
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05316/key=9/data.jsonl', format = JSONEachRow) SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
SELECT key, _row_number FROM s3(s3_conn, filename = currentDatabase() || '/05316/key=9/data.jsonl', format = JSONEachRow);
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05316/key=9/data.jsonl', format = JSONEachRow) SETTINGS optimize_count_from_files = 0, use_hive_partitioning = 0;

SELECT count() FROM file(currentDatabase() || '/05316/key=9/data.jsonl', JSONEachRow) SETTINGS optimize_count_from_files = 0, use_cache_for_count_from_files = 1;
SELECT count() FROM file(currentDatabase() || '/05316/key=9/data.jsonl', JSONEachRow) SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
SELECT key, _row_number FROM file(currentDatabase() || '/05316/key=9/data.jsonl', JSONEachRow);
SELECT count() FROM file(currentDatabase() || '/05316/key=9/data.jsonl', JSONEachRow) SETTINGS optimize_count_from_files = 0, use_hive_partitioning = 0;

-- The structure lists only partition columns (the path has one more key), so there is no other column to read.
INSERT INTO FUNCTION file(currentDatabase() || '/05316/a=1/key=9/data.jsonl', JSONEachRow, 'key Int64') SETTINGS engine_file_truncate_on_insert = 1 SELECT 9;
SELECT key, count() FROM file(currentDatabase() || '/05316/a=1/key=9/data.jsonl', JSONEachRow, 'key Int64') GROUP BY key SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 0;
SELECT key, count() FROM file(currentDatabase() || '/05316/a=1/key=9/data.jsonl', JSONEachRow, 'key Int64') GROUP BY key SETTINGS optimize_count_from_files = 0;
SELECT key, a, _row_number FROM file(currentDatabase() || '/05316/a=1/key=9/data.jsonl', JSONEachRow, 'key Int64');

-- The values of the partition column in the file differ from the path, the top-K filter must not use them.
-- The file with the larger key is read first, so that the threshold is set before the other file is read.
INSERT INTO FUNCTION file(currentDatabase() || '/05316/b=1/key=10/data.parquet', Parquet, 'key Int64') SELECT 1000 + number FROM numbers(10000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05316/b=1/key=9/data.parquet', Parquet, 'key Int64') SELECT 5000 + number FROM numbers(10000) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
SELECT key FROM file(currentDatabase() || '/05316/b=1/key={10,9}/data.parquet', Parquet, 'key Int64') ORDER BY key LIMIT 3 SETTINGS max_threads = 1, optimize_count_from_files = 0, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_use_native_reader_v3 = 1;

-- Nor may a filter on the partition column use them.
SELECT count() FROM file(currentDatabase() || '/05316/b=1/key=9/data.parquet', Parquet, 'key Int64') WHERE key = 9 SETTINGS optimize_count_from_files = 0;
INSERT INTO FUNCTION file(currentDatabase() || '/05316/c=1/key=9/data.orc', ORC, 'key Int64') SELECT 10 FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;
SELECT count() FROM file(currentDatabase() || '/05316/c=1/key=9/data.orc', ORC, 'key Int64') WHERE key = 9 SETTINGS optimize_count_from_files = 0, input_format_orc_filter_push_down = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05316/c=1/key=9/data.orc', format = ORC, structure = 'key Int64') SETTINGS s3_truncate_on_insert = 1 SELECT 10 FROM numbers(10);
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05316/c=1/key=9/data.orc', format = ORC, structure = 'key Int64') WHERE key = 9 SETTINGS optimize_count_from_files = 0, input_format_orc_filter_push_down = 1;

-- Nor may a row policy on the partition column, on a `File` or an `S3` table.
CREATE TABLE t_05316_file AS file(currentDatabase() || '/05316/b=1/key=9/data.parquet', Parquet, 'key Int64');
CREATE TABLE t_05316_s3 (key Int64) ENGINE = S3(s3_conn, filename = currentDatabase() || '/05316/c=1/key=9/data.orc', format = ORC);
CREATE ROW POLICY p_05316_file ON t_05316_file USING key = 9 TO ALL;
CREATE ROW POLICY p_05316_s3 ON t_05316_s3 USING key = 9 TO ALL;
SELECT key, count() FROM t_05316_file GROUP BY key SETTINGS optimize_count_from_files = 0;
SELECT key, count() FROM t_05316_s3 GROUP BY key SETTINGS optimize_count_from_files = 0, input_format_orc_filter_push_down = 1;
ALTER ROW POLICY p_05316_file ON t_05316_file USING key = 10;
ALTER ROW POLICY p_05316_s3 ON t_05316_s3 USING key = 10;
SELECT count() FROM t_05316_file SETTINGS optimize_count_from_files = 0;
SELECT count() FROM t_05316_s3 SETTINGS optimize_count_from_files = 0, input_format_orc_filter_push_down = 1;
DROP ROW POLICY p_05316_file ON t_05316_file;
DROP ROW POLICY p_05316_s3 ON t_05316_s3;

INSERT INTO FUNCTION file(currentDatabase() || '/05316/key=9/data.native', Native) SETTINGS engine_file_truncate_on_insert = 1 SELECT 'a' AS s;
SELECT count() FROM file(currentDatabase() || '/05316/key=9/data.native', Native) SETTINGS optimize_count_from_files = 0;
