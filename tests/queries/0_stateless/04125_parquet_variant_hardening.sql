-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET max_parser_depth = 64;

INSERT INTO FUNCTION file(currentDatabase() || '04125_parquet_variant_hardening_depth.parquet', Parquet)
SELECT materialize('{"a":{"b":{"c":{"d":{"e":{"f":1}}}}}}')::JSON(a Tuple(b Tuple(c Tuple(d Tuple(e Tuple(f UInt64)))))) AS j;

DESC file(currentDatabase() || '04125_parquet_variant_hardening_depth.parquet', Parquet)
SETTINGS max_parser_depth = 5; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE, TOO_DEEP_RECURSION }

INSERT INTO FUNCTION file(currentDatabase() || '04125_parquet_variant_hardening_value_string_limit.parquet', Parquet)
SELECT materialize('{"a":"abcdef"}')::JSON(max_dynamic_paths=0) AS j;

SELECT j
FROM file(
    currentDatabase() || '04125_parquet_variant_hardening_value_string_limit.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=0)')
SETTINGS format_binary_max_string_size = 5
FORMAT Null; -- { serverError TOO_LARGE_STRING_SIZE }

INSERT INTO FUNCTION file(currentDatabase() || '04125_parquet_variant_hardening_metadata_string_limit.parquet', Parquet)
SELECT materialize('{"abcdef":1}')::JSON(max_dynamic_paths=0) AS j;

SELECT j
FROM file(
    currentDatabase() || '04125_parquet_variant_hardening_metadata_string_limit.parquet',
    Parquet,
    'j JSON(max_dynamic_paths=0)')
SETTINGS format_binary_max_string_size = 5
FORMAT Null; -- { serverError TOO_LARGE_STRING_SIZE }

INSERT INTO FUNCTION file(currentDatabase() || '04125_parquet_variant_hardening_prewhere_subcolumns.parquet', Parquet)
SELECT materialize('{"a":1,"b":"x"}')::JSON AS j
UNION ALL
SELECT materialize('{"a":2,"b":"y"}')::JSON AS j;

SELECT j.a.:Int64, j.b.:String
FROM file(
    currentDatabase() || '04125_parquet_variant_hardening_prewhere_subcolumns.parquet',
    Parquet,
    'j JSON')
PREWHERE position(toJSONString(j), '"a":2') > 0
FORMAT TSVRaw;
