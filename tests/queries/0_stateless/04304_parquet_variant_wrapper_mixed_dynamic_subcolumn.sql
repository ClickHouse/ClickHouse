-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04304_parquet_variant_wrapper_mixed_dynamic_subcolumn.parquet', Parquet)
SELECT CAST(raw AS JSON(max_dynamic_paths=1)) AS j
FROM values(
    'raw String',
    ('{"b":1}'),
    ('{"b":{"c":2}}'),
    ('{"b":3}'));

SELECT toTypeName(j.@b), dynamicType(j.@b), toString(j.@b)
FROM file(
    currentDatabase() || '04304_parquet_variant_wrapper_mixed_dynamic_subcolumn.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=1)')
ORDER BY toString(j.@b)
FORMAT TSVRaw;
