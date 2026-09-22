-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04118_parquet_json_as_variant_plain.parquet', Parquet)
SELECT CAST('{"a":1,"b":2}' AS JSON(max_dynamic_paths=1)) AS j
UNION ALL
SELECT CAST('{"b":3}' AS JSON(max_dynamic_paths=1)) AS j;

SELECT 'plain', tupleElement(column, 'path')
FROM file(currentDatabase() || '04118_parquet_json_as_variant_plain.parquet', ParquetMetadata)
ARRAY JOIN columns AS column
ORDER BY tupleElement(column, 'path')
FORMAT TSVRaw;

SELECT 'plain', j.a.:Int64, j.b.:Int64
FROM file(currentDatabase() || '04118_parquet_json_as_variant_plain.parquet', Parquet, 'j JSON(max_dynamic_paths=1)')
ORDER BY ifNull(j.b.:Int64, 0)
FORMAT TSVRaw;

SET output_format_parquet_json_as_variant = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04118_parquet_json_as_variant_variant.parquet', Parquet)
SELECT CAST('{"a":1,"b":2}' AS JSON(max_dynamic_paths=1)) AS j
UNION ALL
SELECT CAST('{"b":3}' AS JSON(max_dynamic_paths=1)) AS j;

SELECT 'variant', tupleElement(column, 'path')
FROM file(currentDatabase() || '04118_parquet_json_as_variant_variant.parquet', ParquetMetadata)
ARRAY JOIN columns AS column
ORDER BY tupleElement(column, 'path')
FORMAT TSVRaw;

SELECT 'variant', j.a.:Int64, j.b.:Int64
FROM file(currentDatabase() || '04118_parquet_json_as_variant_variant.parquet', Parquet, 'j JSON(max_dynamic_paths=1)')
ORDER BY ifNull(j.b.:Int64, 0)
FORMAT TSVRaw;
