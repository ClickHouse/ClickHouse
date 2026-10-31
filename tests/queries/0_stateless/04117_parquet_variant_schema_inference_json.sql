-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04117_parquet_variant_schema_inference_json.parquet', Parquet)
SELECT CAST(CAST('{"a":42}' AS JSON) AS Dynamic) AS d
UNION ALL
SELECT CAST(CAST('{"a":"x"}' AS JSON) AS Dynamic);

SET input_format_parquet_enable_json_parsing = 0;
SELECT toTypeName(d)
FROM file(currentDatabase() || '04117_parquet_variant_schema_inference_json.parquet', Parquet)
LIMIT 1
FORMAT TSVRaw;

SET input_format_parquet_enable_json_parsing = 1;
SELECT toTypeName(d)
FROM file(currentDatabase() || '04117_parquet_variant_schema_inference_json.parquet', Parquet)
LIMIT 1
FORMAT TSVRaw;

SELECT toTypeName(d), dynamicType(d), toString(d)
FROM file(currentDatabase() || '04117_parquet_variant_schema_inference_json.parquet', Parquet)
ORDER BY dynamicType(d), toString(d)
FORMAT TSVRaw;
