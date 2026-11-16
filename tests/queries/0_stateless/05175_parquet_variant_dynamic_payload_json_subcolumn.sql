-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: requesting a `JSON` path subcolumn of a `Parquet` column whose payload is
-- parsed as `Dynamic` used to throw a `LOGICAL_ERROR` ("Cannot extract subcolumn ... from parsed
-- `Parquet` object column ...") - `Dynamic` has type-name subcolumns, not `JSON` paths. It must
-- be a regular user-facing error instead. With `schema_inference_make_columns_nullable`, the same
-- read used to build an illegal `Nullable(Dynamic)` and fail with `ILLEGAL_TYPE_OF_ARGUMENT`.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET enable_time_time64_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '05175_dynamic_scalar.parquet', Parquet)
SELECT CAST('123', 'Dynamic') AS j;

SELECT '-- The whole column still reads back as `Dynamic`';
SELECT j, toTypeName(j) FROM file(currentDatabase() || '05175_dynamic_scalar.parquet', Parquet, 'j Dynamic');

SELECT '-- A `JSON` path subcolumn of a `Dynamic` payload is a user error, not a logical error';
SELECT j.t FROM file(currentDatabase() || '05175_dynamic_scalar.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(6))') SETTINGS schema_inference_make_columns_nullable = 0; -- { serverError ILLEGAL_COLUMN }
SELECT j.t FROM file(currentDatabase() || '05175_dynamic_scalar.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(6))') SETTINGS schema_inference_make_columns_nullable = 1; -- { serverError ILLEGAL_COLUMN }

SELECT '-- A `JSON` object payload stored as `Dynamic` still exposes its paths';
INSERT INTO FUNCTION file(currentDatabase() || '05175_dynamic_object.parquet', Parquet)
SELECT CAST(CAST('{"t":"00:01:02.345678"}', 'JSON'), 'Dynamic') AS j;
SELECT j, j.t, toTypeName(j.t) FROM file(currentDatabase() || '05175_dynamic_object.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(6))');
