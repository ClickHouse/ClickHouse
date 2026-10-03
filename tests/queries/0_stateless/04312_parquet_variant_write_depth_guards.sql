-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;

DROP TABLE IF EXISTS test_04312_parquet_variant_write_depth_guards;
DROP TABLE IF EXISTS test_04312_parquet_variant_write_depth_guards_dynamic;

CREATE TABLE test_04312_parquet_variant_write_depth_guards
(
    j JSON(max_dynamic_paths=0, a UInt64)
)
ENGINE = Memory;

SET max_parser_depth = 64;

INSERT INTO test_04312_parquet_variant_write_depth_guards
SELECT CAST('{"a":1,"b":{"c":{"d":{"e":{"f":2}}}}}' AS JSON(max_dynamic_paths=0, a UInt64));

INSERT INTO FUNCTION file(currentDatabase() || '04312_parquet_variant_write_depth_guards_direct.parquet', Parquet)
SELECT j
FROM test_04312_parquet_variant_write_depth_guards
SETTINGS max_parser_depth = 5; -- { serverError TOO_DEEP_RECURSION }

TRUNCATE TABLE test_04312_parquet_variant_write_depth_guards;

SET max_parser_depth = 64;

INSERT INTO test_04312_parquet_variant_write_depth_guards
SELECT CAST('{"a":1,"b":{"c":{"d":{"e":{"f":[2]}}}}}' AS JSON(max_dynamic_paths=0, a UInt64));

INSERT INTO FUNCTION file(currentDatabase() || '04312_parquet_variant_write_depth_guards_fallback.parquet', Parquet)
SELECT j
FROM test_04312_parquet_variant_write_depth_guards
SETTINGS max_parser_depth = 5; -- { serverError TOO_DEEP_RECURSION }

DROP TABLE test_04312_parquet_variant_write_depth_guards;

CREATE TABLE test_04312_parquet_variant_write_depth_guards_dynamic
(
    d Dynamic
)
ENGINE = Memory;

SET max_parser_depth = 64;

INSERT INTO test_04312_parquet_variant_write_depth_guards_dynamic
SELECT CAST([[[[[[toInt64(1)]]]]]], 'Dynamic');

INSERT INTO FUNCTION file(currentDatabase() || '04312_parquet_variant_write_depth_guards_dynamic.parquet', Parquet)
SELECT d
FROM test_04312_parquet_variant_write_depth_guards_dynamic
SETTINGS max_parser_depth = 5; -- { serverError TOO_DEEP_RECURSION }

DROP TABLE test_04312_parquet_variant_write_depth_guards_dynamic;
