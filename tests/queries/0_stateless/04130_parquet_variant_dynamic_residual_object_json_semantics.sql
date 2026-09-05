-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET output_format_parquet_json_as_variant = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04130_parquet_variant_dynamic_residual_object_json_semantics.parquet', Parquet)
SELECT CAST('{"a":1,"b":{"c":2}}' AS JSON(max_dynamic_paths=0)) AS j
UNION ALL
SELECT CAST('{"a":3}' AS JSON(max_dynamic_paths=0)) AS j;

SELECT startsWith(dynamicType(j), 'JSON'), toString(j)
FROM file(
    currentDatabase() || '04130_parquet_variant_dynamic_residual_object_json_semantics.parquet',
    Parquet,
    'j Dynamic')
ORDER BY toString(j)
FORMAT TSVRaw;
