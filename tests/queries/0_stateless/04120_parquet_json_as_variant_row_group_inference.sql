-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET output_format_parquet_row_group_size = 1;
SET max_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04120_parquet_json_as_variant_row_group_inference.parquet', Parquet)
SELECT CAST(raw AS JSON(max_dynamic_paths=1)) AS j
FROM values(
    'raw String',
    ('{"a":1}'),
    ('{"b":2}'),
    ('{"b":3}'));

SELECT tupleElement(column, 'path')
FROM file(currentDatabase() || '04120_parquet_json_as_variant_row_group_inference.parquet', ParquetMetadata)
ARRAY JOIN columns AS column
ORDER BY tupleElement(column, 'path')
FORMAT TSVRaw;

SELECT j.a.:Int64, j.b.:Int64
FROM file(
    currentDatabase() || '04120_parquet_json_as_variant_row_group_inference.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=1)')
ORDER BY ifNull(j.b.:Int64, 0), ifNull(j.a.:Int64, 0)
FORMAT TSVRaw;
