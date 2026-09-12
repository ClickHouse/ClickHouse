-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: a `Dynamic` row holding a `Map` with a non-`String` key type must be rejected
-- with `NOT_IMPLEMENTED` by the shredding analysis pass as well. `analyzeVariantColumnarValue` used
-- to `assert_cast` the key column to `ColumnString`, which in release builds degrades to a raw
-- `static_cast` and misinterprets e.g. a `ColumnUInt64` instead of throwing a clean exception.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;

SELECT '-- Dynamic holding Map(UInt64, String)';
INSERT INTO FUNCTION file(currentDatabase() || '04665_variant_map_uint_key.parquet', Parquet)
SELECT CAST(map(1, 'x'), 'Dynamic') AS d; -- { serverError NOT_IMPLEMENTED }

SELECT '-- Dynamic holding Map(Date, String)';
INSERT INTO FUNCTION file(currentDatabase() || '04665_variant_map_date_key.parquet', Parquet)
SELECT CAST(map(toDate('2026-08-01'), 'x'), 'Dynamic') AS d; -- { serverError NOT_IMPLEMENTED }

SELECT '-- Dynamic holding Map(String, String) still works';
INSERT INTO FUNCTION file(currentDatabase() || '04665_variant_map_string_key.parquet', Parquet)
SELECT CAST(map('a', 'x'), 'Dynamic') AS d;

SELECT count()
FROM file(currentDatabase() || '04665_variant_map_string_key.parquet', Parquet, 'd Dynamic');
