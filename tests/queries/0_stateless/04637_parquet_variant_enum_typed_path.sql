-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: exact enum scalars must not silently widen to `Int64` when written as
-- shredded `VARIANT` (`tryConvertVariantScalarToShreddedField` used to skip `Enum8`/`Enum16`,
-- so the numeric `Field` fallback changed `dynamicType` and the inferred schema on round trip).

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET output_format_parquet_json_as_variant = 1;
SET enable_json_type = 1;
SET allow_experimental_dynamic_type = 1;
SET output_format_parquet_enum_as_byte_array = 1;

SELECT '-- Dynamic holding Enum8 scalars';
INSERT INTO FUNCTION file(currentDatabase() || '04637_variant_enum8_dynamic.parquet', Parquet)
SELECT CAST(CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic) AS d
UNION ALL
SELECT CAST(CAST('b' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic);

SELECT d, dynamicType(d)
FROM file(currentDatabase() || '04637_variant_enum8_dynamic.parquet', Parquet, 'd Dynamic')
ORDER BY toString(d)
FORMAT TSVRaw;

SELECT '-- Dynamic holding Enum16 scalars';
INSERT INTO FUNCTION file(currentDatabase() || '04637_variant_enum16_dynamic.parquet', Parquet)
SELECT CAST(CAST('x' AS Enum16('x' = 300, 'y' = 400)) AS Dynamic) AS d
UNION ALL
SELECT CAST(CAST('y' AS Enum16('x' = 300, 'y' = 400)) AS Dynamic);

SELECT d, dynamicType(d)
FROM file(currentDatabase() || '04637_variant_enum16_dynamic.parquet', Parquet, 'd Dynamic')
ORDER BY toString(d)
FORMAT TSVRaw;

SELECT '-- Dynamic holding Enum8 scalars, numeric representation';
SET output_format_parquet_enum_as_byte_array = 0;
INSERT INTO FUNCTION file(currentDatabase() || '04637_variant_enum8_numeric.parquet', Parquet)
SELECT CAST(CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic) AS d
UNION ALL
SELECT CAST(CAST('b' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic);

SELECT d, dynamicType(d)
FROM file(currentDatabase() || '04637_variant_enum8_numeric.parquet', Parquet, 'd Dynamic')
ORDER BY toString(d)
FORMAT TSVRaw;
SET output_format_parquet_enum_as_byte_array = 1;

SELECT '-- JSON typed path declared as enum';
INSERT INTO FUNCTION file(currentDatabase() || '04637_variant_enum_json.parquet', Parquet)
SELECT CAST('{"kind":"a","v":1}' AS JSON(kind Enum8('a' = 1, 'b' = 2))) AS json
UNION ALL
SELECT CAST('{"kind":"b","v":2}' AS JSON(kind Enum8('a' = 1, 'b' = 2)));

SELECT json.kind, toTypeName(json.kind), json.v
FROM file(
    currentDatabase() || '04637_variant_enum_json.parquet',
    Parquet,
    'json JSON(kind Enum8(''a'' = 1, ''b'' = 2))')
ORDER BY json.kind
FORMAT TSVRaw;
