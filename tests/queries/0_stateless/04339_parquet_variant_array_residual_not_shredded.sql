-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET engine_file_truncate_on_insert = 1;
SET schema_inference_make_columns_nullable = 0;

INSERT INTO FUNCTION file(currentDatabase() || '04339_parquet_variant_array_residual_not_shredded.parquet', Parquet)
SELECT CAST(raw AS JSON(max_dynamic_paths=1)) AS j
FROM values(
    'raw String',
    ('{"a":[1]}'),
    ('{"a":[2]}'),
    ('{"a":42}'));

SELECT tupleElement(column, 'path')
FROM file(currentDatabase() || '04339_parquet_variant_array_residual_not_shredded.parquet', ParquetMetadata)
ARRAY JOIN columns AS column
ORDER BY tupleElement(column, 'path')
FORMAT TSVRaw;

SELECT toTypeName(j), j.a.:Int64, toJSONString(j)
FROM file(currentDatabase() || '04339_parquet_variant_array_residual_not_shredded.parquet', Parquet)
ORDER BY toJSONString(j)
FORMAT TSVRaw;
