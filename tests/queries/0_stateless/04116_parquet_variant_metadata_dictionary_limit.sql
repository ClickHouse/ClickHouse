-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04116_parquet_variant_metadata_dictionary_limit.parquet', Parquet)
SELECT materialize('{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6}')::JSON(max_dynamic_paths=0) AS json;

SELECT json
FROM file(
    currentDatabase() || '04116_parquet_variant_metadata_dictionary_limit.parquet',
    Parquet,
    'json JSON(max_dynamic_paths=0)')
SETTINGS format_binary_max_object_size = 5
FORMAT Null; -- { serverError LIMIT_EXCEEDED }
