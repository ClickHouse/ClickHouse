-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: the `Parquet` dictionary-filter push-down materializes the dictionary values into
-- a column of `PrimitiveColumnInfo::decoded_type`, but for a column read as `LowCardinality` that
-- type carries the `LowCardinality` wrapper while `Dictionary` stores plain values. The resulting
-- `ColumnLowCardinality` was then `assert_cast` to `ColumnString`, which in release builds is a raw
-- `static_cast` and passed a garbage size to the allocator.

SET input_format_parquet_use_native_reader_v3 = 1;
SET engine_file_truncate_on_insert = 1;
SET enable_json_type = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04666_lc_dictionary_filter.parquet', Parquet)
SELECT 'c' || toString(number % 5) AS s, number AS n FROM numbers(1000);

SELECT '-- LowCardinality(String) with dictionary filter push-down';
SELECT count()
FROM file(currentDatabase() || '04666_lc_dictionary_filter.parquet', Parquet, 's LowCardinality(String), n UInt64')
WHERE s = 'c3';

SELECT '-- value that is not in the dictionary';
SELECT count()
FROM file(currentDatabase() || '04666_lc_dictionary_filter.parquet', Parquet, 's LowCardinality(String), n UInt64')
WHERE s = 'nope';

SELECT '-- LowCardinality(Nullable(String))';
SELECT count()
FROM file(currentDatabase() || '04666_lc_dictionary_filter.parquet', Parquet, 's LowCardinality(Nullable(String)), n UInt64')
WHERE s = 'c3';

SELECT '-- LowCardinality typed path of a shredded VARIANT';
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
INSERT INTO FUNCTION file(currentDatabase() || '04666_lc_dictionary_filter_variant.parquet', Parquet)
SELECT CAST(concat('{"kind":"c', toString(number % 5), '","n":', toString(number), '}') AS JSON(kind LowCardinality(String), n UInt64)) AS data
FROM numbers(1000);

SELECT count()
FROM file(
    currentDatabase() || '04666_lc_dictionary_filter_variant.parquet',
    Parquet,
    'data JSON(max_dynamic_paths=0, kind LowCardinality(String), n UInt64)')
WHERE data.kind = 'c3';
