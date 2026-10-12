-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04129_parquet_variant_dynamic_complex_roundtrip.parquet', Parquet)
SELECT CAST([toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC')], 'Dynamic') AS d
UNION ALL
SELECT CAST([toInt64(42)], 'Dynamic');

SELECT toTypeName(d)
FROM file(currentDatabase() || '04129_parquet_variant_dynamic_complex_roundtrip.parquet', Parquet)
SETTINGS schema_inference_make_columns_nullable = 0;

SELECT dynamicType(d), toString(d)
FROM file(currentDatabase() || '04129_parquet_variant_dynamic_complex_roundtrip.parquet', Parquet)
ORDER BY dynamicType(d)
SETTINGS schema_inference_make_columns_nullable = 0;

INSERT INTO FUNCTION file(currentDatabase() || '04129_parquet_variant_dynamic_named_tuple_roundtrip.parquet', Parquet)
SELECT CAST(CAST(tuple(toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC')) AS Tuple(ts DateTime64(6, 'UTC'))), 'Dynamic') AS d;

SELECT position(toString(d), '2024-01-02 03:04:05.123456') > 0
FROM file(currentDatabase() || '04129_parquet_variant_dynamic_named_tuple_roundtrip.parquet', Parquet)
SETTINGS schema_inference_make_columns_nullable = 0;
