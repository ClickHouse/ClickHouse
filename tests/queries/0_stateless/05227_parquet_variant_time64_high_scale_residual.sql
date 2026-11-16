-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: the residual `Parquet` `VARIANT` encoding of a `Time64` value with a scale above 6
-- used to rescale the value to microseconds and write `TIME(MICROS)`, silently dropping the extra
-- fractional digits. `VARIANT` has no finer time-of-day primitive, so such values are now written as a
-- `DECIMAL8` with the value's own scale, which is lossless: it reads back as `Decimal(18, scale)` and
-- converts to the `Time64` type of a declared path or a type hint. Scales up to 6 keep `TIME(MICROS)`.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET enable_time_time64_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET use_legacy_to_time = 0;

SELECT '-- Declared `Time64(9)` typed path keeps all nine fractional digits';
INSERT INTO FUNCTION file(currentDatabase() || '_05227_time64_9.parquet', Parquet)
SELECT CAST('{"t":"00:00:00.123456789"}', 'JSON(max_dynamic_paths=0, t Time64(9))') AS j;
SELECT j.t, toTypeName(j.t) FROM file(currentDatabase() || '_05227_time64_9.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(9))');
SELECT j FROM file(currentDatabase() || '_05227_time64_9.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(9))');
SELECT '-- The same file read back as plain JSON exposes the residual decimal';
SELECT j, JSONAllPathsWithTypes(j) FROM file(currentDatabase() || '_05227_time64_9.parquet', Parquet, 'j JSON');

SELECT '-- Declared `Time64(7)` typed path';
INSERT INTO FUNCTION file(currentDatabase() || '_05227_time64_7.parquet', Parquet)
SELECT CAST('{"t":"12:34:56.1234567"}', 'JSON(max_dynamic_paths=0, t Time64(7))') AS j;
SELECT j.t, toTypeName(j.t) FROM file(currentDatabase() || '_05227_time64_7.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(7))');

SELECT '-- `Time64(6)` still uses the `TIME(MICROS)` primitive';
INSERT INTO FUNCTION file(currentDatabase() || '_05227_time64_6.parquet', Parquet)
SELECT CAST('{"t":"12:34:56.123456"}', 'JSON(max_dynamic_paths=0, t Time64(6))') AS j;
SELECT j.t, toTypeName(j.t) FROM file(currentDatabase() || '_05227_time64_6.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(6))');
SELECT j, JSONAllPathsWithTypes(j) FROM file(currentDatabase() || '_05227_time64_6.parquet', Parquet, 'j JSON');

SELECT '-- `Dynamic` values: mixed types force the residual path; nothing is truncated';
INSERT INTO FUNCTION file(currentDatabase() || '_05227_time64_dynamic.parquet', Parquet)
SELECT arrayJoin([
    CAST(CAST('23:59:59.999999999', 'Time64(9)'), 'Dynamic'),
    CAST(CAST('-00:00:00.000000001', 'Time64(9)'), 'Dynamic'),
    CAST(CAST('00:00:01.5', 'Time64(6)'), 'Dynamic'),
    CAST('x', 'Dynamic')]) AS d;
SELECT d, dynamicType(d) FROM file(currentDatabase() || '_05227_time64_dynamic.parquet', Parquet, 'd Dynamic');
