-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04306_parquet_variant_partial_shredding_projection.parquet', Parquet)
SELECT CAST('{"a":1,"b":2}' AS JSON(max_dynamic_paths=1)) AS j
UNION ALL
SELECT CAST('{"a":4,"b":3}' AS JSON(max_dynamic_paths=1)) AS j;

SELECT 'residual-only', j.b.:Int64
FROM file(
    currentDatabase() || '04306_parquet_variant_partial_shredding_projection.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=1)')
ORDER BY j.b.:Int64
FORMAT TSVRaw;

SELECT 'typed-only', j.a.:Int64
FROM file(
    currentDatabase() || '04306_parquet_variant_partial_shredding_projection.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=1)')
ORDER BY j.a.:Int64
FORMAT TSVRaw;

SELECT 'mixed', j.a.:Int64, j.b.:Int64
FROM file(
    currentDatabase() || '04306_parquet_variant_partial_shredding_projection.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=1)')
ORDER BY j.a.:Int64
FORMAT TSVRaw;
