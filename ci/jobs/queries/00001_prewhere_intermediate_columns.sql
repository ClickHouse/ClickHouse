-- FIXME
-- Tags: xfail
INSERT INTO FUNCTION file('00001_prewhere_intermediate_columns.parquet') SETTINGS engine_file_truncate_on_insert = 1, max_threads = 1, output_format_parquet_row_group_size = 5 SELECT number * 10 AS x FROM numbers(20) SETTINGS max_threads = 1, output_format_parquet_row_group_size = 5;
-- Reproduces prewhere optimization bug where intermediate columns are kept in outputs:
-- 1. Virtual column (_row_number) in SELECT affects optimizer cost calculations
-- 2. Same expression (x > 60) used in both WHERE and ORDER BY
-- 3. Optimizer preserves intermediate greater(x, 60) column to avoid recomputation
-- 4. This intermediate column appears in format_header as a new column, triggering the bug
SELECT DISTINCT x, _row_number FROM file(toNullable('00001_prewhere_intermediate_columns.parquet')) WHERE (x > 60) AND (x < 100) ORDER BY (x > 60), x;
