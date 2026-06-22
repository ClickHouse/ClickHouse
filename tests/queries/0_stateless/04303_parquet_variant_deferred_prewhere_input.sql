-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04303_parquet_variant_deferred_prewhere_input.parquet', Parquet)
SELECT CAST('{"kind":"commit","extra":"x"}' AS JSON(max_dynamic_paths=0, kind String)) AS j
UNION ALL
SELECT CAST('{"kind":"identity","extra":"y"}' AS JSON(max_dynamic_paths=0, kind String)) AS j
UNION ALL
SELECT CAST('{"kind":"commit","extra":"x"}' AS JSON(max_dynamic_paths=0, kind String)) AS j;

SELECT count()
FROM file(
    currentDatabase() || '04303_parquet_variant_deferred_prewhere_input.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=0, kind String)')
PREWHERE j.kind = 'commit'
FORMAT TSVRaw;

SELECT 'parent-prewhere', JSONExtractString(toJSONString(j), 'extra')
FROM file(
    currentDatabase() || '04303_parquet_variant_deferred_prewhere_input.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=0, kind String)')
PREWHERE j.kind = 'identity'
FORMAT TSVRaw;
