-- Tags: no-fasttest
-- Parquet filter pushdown should not throw when used with Dynamic/Variant/JSON columns.
-- The stats-based row group filtering is skipped for these types because Parquet
-- physical-type statistics are not meaningful for heterogeneous types.
-- https://github.com/ClickHouse/ClickHouse/issues/87695

SET input_format_parquet_filter_push_down = 1;

INSERT INTO FUNCTION file('04050_dynamic_' || currentDatabase() || '.parquet') SELECT toString(number) AS c0 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

SELECT count() FROM file('04050_dynamic_' || currentDatabase() || '.parquet', Parquet, 'c0 Dynamic') WHERE c0 = '1';
SELECT count() FROM file('04050_dynamic_' || currentDatabase() || '.parquet', Parquet, 'c0 Dynamic') WHERE c0 = '99';
SELECT count() FROM file('04050_dynamic_' || currentDatabase() || '.parquet', Parquet, 'c0 Dynamic') WHERE c0 = 'nonexistent';
SELECT count() FROM file('04050_dynamic_' || currentDatabase() || '.parquet', Parquet, 'c0 Variant(String, UInt64)') WHERE c0 = '1';
SELECT count() FROM file('04050_dynamic_' || currentDatabase() || '.parquet', Parquet, 'c0 JSON') WHERE c0 IS NOT NULL;
