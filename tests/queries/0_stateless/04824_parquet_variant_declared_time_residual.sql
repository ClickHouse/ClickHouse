-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: a declared shredded path of type `Time` / `Time64` has no shredded
-- `typed_value` leaf mapping in the `Parquet` writer (`tryConvertVariantScalarToShreddedField`
-- cannot convert it and `preparePrimitiveColumn` has no leaf for it), so it must be rejected
-- as a declared shredded type up front and written through the residual `value` encoding
-- instead of failing with `UNKNOWN_TYPE` on the synthetic `typed_value` column.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET enable_time_time64_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET use_legacy_to_time = 0;

SELECT '-- Declared `Time64(6)` typed path is written through residual encoding';
INSERT INTO FUNCTION file(currentDatabase() || '04824_variant_declared_time64.parquet', Parquet)
SELECT CAST('{"t":"00:01:02.345678"}', 'JSON(max_dynamic_paths=0, t Time64(6))') AS j;
SELECT j FROM file(currentDatabase() || '04824_variant_declared_time64.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(6))');
SELECT j.t, toTypeName(j.t) FROM file(currentDatabase() || '04824_variant_declared_time64.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time64(6))');

SELECT '-- The same file read back as plain JSON';
SELECT j, JSONAllPathsWithTypes(j) FROM file(currentDatabase() || '04824_variant_declared_time64.parquet', Parquet, 'j JSON');

SELECT '-- Declared `Time` typed path is written through residual encoding';
INSERT INTO FUNCTION file(currentDatabase() || '04824_variant_declared_time.parquet', Parquet)
SELECT CAST('{"t":"00:01:02"}', 'JSON(max_dynamic_paths=0, t Time)') AS j;
SELECT j FROM file(currentDatabase() || '04824_variant_declared_time.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time)');
SELECT j.t, toTypeName(j.t) FROM file(currentDatabase() || '04824_variant_declared_time.parquet', Parquet, 'j JSON(max_dynamic_paths=0, t Time)');
