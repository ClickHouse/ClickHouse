-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- `max_parser_depth = 0` means "unlimited" for the `Parquet` `VARIANT` depth guards, exactly like
-- it does for the SQL parser. The only backstop left in that mode is `checkStackSize`, which both
-- `checkVariantReadDepth` and `checkVariantWriteDepth` now call on every recursion level, so a
-- deeply nested payload trips `TOO_DEEP_RECURSION` instead of running the native stack out.
-- A payload deep enough to actually exhaust the stack cannot be built from SQL (the `JSON` parser
-- refuses nesting deeper than about 1024 levels), so what is pinned here is that a finite
-- `max_parser_depth` still throws while `max_parser_depth = 0` reads and writes the same value.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET max_parser_depth = 64;

CREATE TABLE test_04691_deep_json (j JSON(max_dynamic_paths=0)) ENGINE = Memory;

INSERT INTO test_04691_deep_json
SELECT CAST(repeat('{"a":', 16) || '1' || repeat('}', 16) AS JSON(max_dynamic_paths=0));

-- A finite limit still applies on the write path.
INSERT INTO FUNCTION file(currentDatabase() || '04691_parquet_variant_depth_zero.parquet', Parquet)
SELECT j FROM test_04691_deep_json
SETTINGS max_parser_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- `max_parser_depth = 0` is unlimited, so the same value is written.
INSERT INTO FUNCTION file(currentDatabase() || '04691_parquet_variant_depth_zero.parquet', Parquet)
SELECT j FROM test_04691_deep_json
SETTINGS max_parser_depth = 0;

-- A finite limit still applies on the read path.
SELECT j
FROM file(
    currentDatabase() || '04691_parquet_variant_depth_zero.parquet',
    Parquet,
    'j String')
SETTINGS max_parser_depth = 5
FORMAT Null; -- { serverError TOO_DEEP_RECURSION }

-- `max_parser_depth = 0` is unlimited, so the value reads back unchanged.
SELECT j
FROM file(
    currentDatabase() || '04691_parquet_variant_depth_zero.parquet',
    Parquet,
    'j String')
SETTINGS max_parser_depth = 0;

DROP TABLE test_04691_deep_json;
