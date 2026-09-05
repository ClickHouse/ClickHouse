-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET max_parser_depth = 64;

INSERT INTO FUNCTION file(currentDatabase() || '04113_parquet_variant_read_depth_residual.parquet', Parquet)
SELECT materialize('{"a":{"a":{"a":{"a":{"a":{"a":{"a":1}}}}}}}')::JSON(max_dynamic_paths=0) AS json;

INSERT INTO FUNCTION file(currentDatabase() || '04113_parquet_variant_read_depth_typed.parquet', Parquet)
SELECT materialize('{"a":{"b":{"c":{"d":{"e":{"f":{"g":1,"h":"x"}}}}}}}')::JSON(a Tuple(b Tuple(c Tuple(d Tuple(e Tuple(f Tuple(g UInt64))))))) AS json;

SELECT json
FROM file(
    currentDatabase() || '04113_parquet_variant_read_depth_residual.parquet',
    Parquet,
    'json String')
SETTINGS max_parser_depth = 5
FORMAT Null; -- { serverError TOO_DEEP_RECURSION }

SELECT json
FROM file(
    currentDatabase() || '04113_parquet_variant_read_depth_typed.parquet',
    Parquet,
    'json Dynamic')
SETTINGS max_parser_depth = 5
FORMAT Null; -- { serverError TOO_DEEP_RECURSION }

SET output_format_parquet_json_as_variant = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04113_parquet_variant_read_depth_direct_scalar.parquet', Parquet)
SELECT CAST('{"a":1}' AS JSON(max_dynamic_paths=0, a UInt64)) AS json;

SELECT json.a.:UInt64
FROM file(
    currentDatabase() || '04113_parquet_variant_read_depth_direct_scalar.parquet',
    Parquet,
    'json JSON(max_dynamic_paths=0, a UInt64)')
SETTINGS max_parser_depth = 1
FORMAT Null; -- { serverError TOO_DEEP_RECURSION }
