-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: a mixed `Dynamic` column that ties an enum with another scalar type
-- (so `constructShreddedType` returns no shredded type) must still be writable as `VARIANT`:
-- the residual `value` encoder used to reject `Enum8`/`Enum16` even though the homogeneous
-- shredded fast path accepted them.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;

SELECT '-- Mixed Enum8/String, names';
SET output_format_parquet_enum_as_byte_array = 1;
INSERT INTO FUNCTION file(currentDatabase() || '04759_variant_enum8_mixed_names.parquet', Parquet)
SELECT CAST(CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic) AS d
UNION ALL
SELECT CAST('x' AS Dynamic);

SELECT d, dynamicType(d)
FROM file(currentDatabase() || '04759_variant_enum8_mixed_names.parquet', Parquet, 'd Dynamic')
ORDER BY toString(d)
FORMAT TSVRaw;

SELECT '-- Mixed Enum8/String, numeric representation';
SET output_format_parquet_enum_as_byte_array = 0;
INSERT INTO FUNCTION file(currentDatabase() || '04759_variant_enum8_mixed_numeric.parquet', Parquet)
SELECT CAST(CAST('b' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic) AS d
UNION ALL
SELECT CAST('x' AS Dynamic);

SELECT d, dynamicType(d)
FROM file(currentDatabase() || '04759_variant_enum8_mixed_numeric.parquet', Parquet, 'd Dynamic')
ORDER BY toString(d)
FORMAT TSVRaw;

SELECT '-- Mixed Enum16/Float64, names';
SET output_format_parquet_enum_as_byte_array = 1;
INSERT INTO FUNCTION file(currentDatabase() || '04759_variant_enum16_mixed_names.parquet', Parquet)
SELECT CAST(CAST('y' AS Enum16('x' = 300, 'y' = 400)) AS Dynamic) AS d
UNION ALL
SELECT CAST(1.5 AS Dynamic);

SELECT d, dynamicType(d)
FROM file(currentDatabase() || '04759_variant_enum16_mixed_names.parquet', Parquet, 'd Dynamic')
ORDER BY toString(d)
FORMAT TSVRaw;

SELECT '-- Mixed Enum16/Float64, numeric representation';
SET output_format_parquet_enum_as_byte_array = 0;
INSERT INTO FUNCTION file(currentDatabase() || '04759_variant_enum16_mixed_numeric.parquet', Parquet)
SELECT CAST(CAST('x' AS Enum16('x' = 300, 'y' = 400)) AS Dynamic) AS d
UNION ALL
SELECT CAST(1.5 AS Dynamic);

SELECT d, dynamicType(d)
FROM file(currentDatabase() || '04759_variant_enum16_mixed_numeric.parquet', Parquet, 'd Dynamic')
ORDER BY toString(d)
FORMAT TSVRaw;

SELECT '-- Enum inside a mixed-type object residual';
SET output_format_parquet_enum_as_byte_array = 1;
INSERT INTO FUNCTION file(currentDatabase() || '04759_variant_enum_in_object.parquet', Parquet)
SELECT CAST(map('e', CAST(CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic)) AS Map(String, Dynamic)) AS d
UNION ALL
SELECT CAST(map('e', CAST('x' AS Dynamic)) AS Map(String, Dynamic));

SELECT d
FROM file(currentDatabase() || '04759_variant_enum_in_object.parquet', Parquet)
ORDER BY toString(d)
FORMAT TSVRaw;
