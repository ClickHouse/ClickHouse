-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: ambiguous scalar `Dynamic` values fall back to residual `value` encoding
-- (`constructShreddedType` returns no shredded type on ties), and the residual encoder used to
-- reject any integer above the signed `Int64` range with `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`.
-- Such values must be preserved through a scale-0 `DECIMAL16` primitive instead: the whole
-- `UInt64` domain and wide integers up to 38 decimal digits round-trip; only wider values error.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

SELECT '-- UInt64 above Int64 range mixed with a string (ambiguous, residual encoding)';
INSERT INTO FUNCTION file(currentDatabase() || '04727_variant_residual_uint64.parquet', Parquet)
SELECT arrayJoin([CAST(18446744073709551615::UInt64 AS Dynamic), CAST('x' AS Dynamic)]) AS d;
SELECT d, dynamicType(d) FROM file(currentDatabase() || '04727_variant_residual_uint64.parquet', Parquet, 'd Dynamic') ORDER BY toString(d);

SELECT '-- UInt64 within Int64 range keeps the INT64 primitive';
INSERT INTO FUNCTION file(currentDatabase() || '04727_variant_residual_uint64_small.parquet', Parquet)
SELECT arrayJoin([CAST(42::UInt64 AS Dynamic), CAST('x' AS Dynamic)]) AS d;
SELECT d, dynamicType(d) FROM file(currentDatabase() || '04727_variant_residual_uint64_small.parquet', Parquet, 'd Dynamic') ORDER BY toString(d);

SELECT '-- Residual Int128 / UInt128 / Int256 / UInt256 within 38 digits are preserved';
INSERT INTO FUNCTION file(currentDatabase() || '04727_variant_residual_wide.parquet', Parquet)
SELECT arrayJoin([
    CAST(99999999999999999999999999999999999999::Int128 AS Dynamic),
    CAST(-99999999999999999999999999999999999999::Int128 AS Dynamic),
    CAST(18446744073709551616::UInt128 AS Dynamic),
    CAST(18446744073709551617::Int256 AS Dynamic),
    CAST(18446744073709551618::UInt256 AS Dynamic),
    CAST('a' AS Dynamic),
    CAST('b' AS Dynamic),
    CAST('c' AS Dynamic),
    CAST('d' AS Dynamic),
    CAST('e' AS Dynamic)]) AS d;
SELECT d, dynamicType(d) FROM file(currentDatabase() || '04727_variant_residual_wide.parquet', Parquet, 'd Dynamic') ORDER BY toString(d);

SELECT '-- Residual wide integers that fit Int64 keep the INT64 primitive';
INSERT INTO FUNCTION file(currentDatabase() || '04727_variant_residual_wide_small.parquet', Parquet)
SELECT arrayJoin([
    CAST(-100::Int128 AS Dynamic),
    CAST('a' AS Dynamic),
    CAST('b' AS Dynamic)]) AS d;
SELECT d, dynamicType(d) FROM file(currentDatabase() || '04727_variant_residual_wide_small.parquet', Parquet, 'd Dynamic') ORDER BY toString(d);

SELECT '-- A residual integer above 38 decimal digits cannot be represented';
INSERT INTO FUNCTION file(currentDatabase() || '04727_variant_residual_too_wide.parquet', Parquet)
SELECT arrayJoin([
    CAST(170141183460469231731687303715884105727::Int128 AS Dynamic),
    CAST('a' AS Dynamic),
    CAST('b' AS Dynamic)]) AS d; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
