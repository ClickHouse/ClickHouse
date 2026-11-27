-- Tags: no-fasttest
SET input_format_parquet_use_native_reader_v3=1;
INSERT INTO FUNCTION file(concat(currentDatabase(), '/03727_prewhere_intermediate_columns.parquet')) SETTINGS engine_file_truncate_on_insert = 1, max_threads = 1, output_format_parquet_row_group_size = 5 SELECT number * 10 AS x FROM numbers(20) SETTINGS max_threads = 1, output_format_parquet_row_group_size = 5;

-- Reproduces prewhere optimization bug where intermediate columns are kept in outputs:
-- 1. Virtual column (_row_number) in SELECT affects optimizer cost calculations
-- 2. Same expression (x < 60) used in both WHERE and ORDER BY
-- 3. Optimizer preserves intermediate less(x, 60) column to avoid recomputation
-- 4. This intermediate column appears in format_header as a new column, triggering the bug
SELECT DISTINCT x, _row_number FROM file(toNullable(concat(currentDatabase(), '/03727_prewhere_intermediate_columns.parquet'))) WHERE (x < 60) OR (x < 100) ORDER BY (x < 60), x SETTINGS input_format_parquet_allow_missing_columns=0, optimize_move_to_prewhere=1;

select x from file(toNullable(concat(currentDatabase(), '/03727_prewhere_intermediate_columns.parquet'))) prewhere x order by x;
