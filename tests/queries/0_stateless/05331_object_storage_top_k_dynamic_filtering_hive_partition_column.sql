-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3

-- The structure lists only a Hive partition column (the path has one more key, `b`), so the format reads it from the file, but the query
-- takes its values from the path. The values in the files differ from the path: the top-K filter must not use them.
-- The file with the larger key is listed first, so that the threshold is set before the other file is read.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05331/b=1/key=10/data.parquet', format = Parquet, structure = 'key Int64') SELECT 1000 + number FROM numbers(10000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05331/b=1/key=9/data.parquet', format = Parquet, structure = 'key Int64') SELECT 5000 + number FROM numbers(10000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

SELECT key FROM s3(s3_conn, filename = currentDatabase() || '/05331/b=1/key={10,9}/data.parquet', format = Parquet, structure = 'key Int64')
ORDER BY key LIMIT 3
SETTINGS use_hive_partitioning = 1, max_threads = 1, max_parsing_threads = 1, optimize_count_from_files = 0, use_query_condition_cache = 0,
    use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_use_native_reader_v3 = 1;
