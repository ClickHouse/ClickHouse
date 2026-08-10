-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

-- Plain (non-declared) `JSON` paths are passed to the `Parquet` reader as direct subcolumn
-- reads instead of reading the whole parent column and extracting in memory.

-- A `VARIANT`-encoded file.
INSERT INTO FUNCTION file(currentDatabase() || '04812_variant.parquet', Parquet)
SELECT CAST(
    multiIf(
        number = 0, '{"a":1,"b":"x"}',
        number = 1, '{"a":2,"nested":{"c":10}}',
        number = 2, '{"b":"y","nested":"scalar"}',
        '{"a":4}')
    AS JSON) AS json
FROM numbers(4)
SETTINGS output_format_parquet_json_as_variant = 1;

-- The same data as an opaque `JSON`-string column.
INSERT INTO FUNCTION file(currentDatabase() || '04812_string.parquet', Parquet)
SELECT CAST(
    multiIf(
        number = 0, '{"a":1,"b":"x"}',
        number = 1, '{"a":2,"nested":{"c":10}}',
        number = 2, '{"b":"y","nested":"scalar"}',
        '{"a":4}')
    AS JSON) AS json
FROM numbers(4)
SETTINGS output_format_parquet_json_as_variant = 0;

SELECT '-- direct dynamic paths, VARIANT file';
SELECT json.a, dynamicType(json.a), json.b, json.nested.c
FROM file(currentDatabase() || '04812_variant.parquet', Parquet, 'json JSON')
ORDER BY toString(json.a)
FORMAT TSVRaw;

SELECT '-- direct dynamic paths, JSON-string file';
SELECT json.a, dynamicType(json.a), json.b, json.nested.c
FROM file(currentDatabase() || '04812_string.parquet', Parquet, 'json JSON')
ORDER BY toString(json.a)
FORMAT TSVRaw;

SELECT '-- overlapping parent and child requests';
SELECT json, json.a
FROM file(currentDatabase() || '04812_variant.parquet', Parquet, 'json JSON')
ORDER BY toString(json.a)
FORMAT TSVRaw;

SELECT '-- overlapping path and nested path requests';
SELECT json.nested, json.nested.c
FROM file(currentDatabase() || '04812_variant.parquet', Parquet, 'json JSON')
ORDER BY toString(json.a)
FORMAT TSVRaw;

-- `PREWHERE` on a plain dynamic path is now within the format-supported column set
-- (it used to be rejected because the path collapsed to the whole parent column).
SELECT '-- PREWHERE pushdown on a dynamic path, VARIANT file';
SELECT count()
FROM file(currentDatabase() || '04812_variant.parquet', Parquet, 'json JSON')
PREWHERE toString(json.a) = '2';

SELECT '-- PREWHERE pushdown on a dynamic path, JSON-string file';
SELECT count()
FROM file(currentDatabase() || '04812_string.parquet', Parquet, 'json JSON')
PREWHERE toString(json.a) = '2';

-- The special accessor forms keep the whole-column fallback and stay correct.
SELECT '-- special accessor subcolumns';
SELECT json.^`nested`, json.@`nested`, json.a.:`Int64`
FROM file(currentDatabase() || '04812_variant.parquet', Parquet, 'json JSON')
ORDER BY toString(json.a)
FORMAT TSVRaw;

-- `Dynamic` type-subcolumns keep the whole-column fallback and stay correct.
INSERT INTO FUNCTION file(currentDatabase() || '04812_dynamic.parquet', Parquet)
SELECT if(number = 0, CAST('str' AS Dynamic), CAST(CAST(number AS Int64) AS Dynamic)) AS d
FROM numbers(2);

SELECT '-- Dynamic type-subcolumns';
SELECT d.String, d.Int64
FROM file(currentDatabase() || '04812_dynamic.parquet', Parquet, 'd Dynamic')
ORDER BY ifNull(d.Int64, toInt64(0))
FORMAT TSVRaw;
