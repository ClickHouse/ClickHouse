-- GeoJSON output: property and id coverage. Every non-geometry/non-id column must serialize as
-- valid JSON inside `properties`.

-- A broad sweep of property column types, all built into one `properties` object.
SELECT
    (1.0, 2.0)::Point AS geometry,
    -7::Int8 AS i8,
    255::UInt8 AS u8,
    123456789012345::Int64 AS i64,
    18446744073709551615::UInt64 AS u64,
    3.5::Float64 AS f64,
    'hello'::String AS s,
    'abc'::FixedString(3) AS fs,
    true AS b,
    toDate('2026-06-20') AS d,
    toDateTime('2026-06-20 12:00:00', 'UTC') AS dt,
    toDateTime64('2026-06-20 12:00:00.123', 3, 'UTC') AS dt64,
    toUUID('00000000-0000-0000-0000-000000000001') AS uuid,
    toDecimal64(1.5, 2) AS dec,
    CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)') AS en,
    [1, 2, 3]::Array(UInt8) AS arr,
    (1, 'x')::Tuple(UInt8, String) AS tup,
    map('k1', 'v1', 'k2', 'v2') AS m,
    CAST(NULL AS Nullable(Int32)) AS n_null,
    CAST(5 AS Nullable(Int32)) AS n_val,
    'lc'::LowCardinality(String) AS lc
FORMAT GeoJSON;

-- Special characters in a property string value are JSON-escaped.
SELECT (1.0, 2.0)::Point AS geometry, 'he said "hi"\t\\ é 日本' AS text FORMAT GeoJSON;

-- A column name needing escaping is emitted as a valid JSON key.
SELECT (1.0, 2.0)::Point AS geometry, 42 AS `a "quoted" \ key` FORMAT GeoJSON;

-- properties is splatted directly from a lone object-typed column named `properties`.
SELECT (1.0, 2.0)::Point AS geometry, '{"a":1,"b":"x"}'::JSON AS properties FORMAT GeoJSON;
SELECT (1.0, 2.0)::Point AS geometry, CAST(NULL AS Nullable(JSON)) AS properties FORMAT GeoJSON;
SELECT (1.0, 2.0)::Point AS geometry, map('a', 'b')::Map(String, String) AS properties FORMAT GeoJSON;
SELECT (1.0, 2.0)::Point AS geometry, (1, 'x')::Tuple(num UInt8, str String) AS properties FORMAT GeoJSON;

-- A non-object column named `properties` is not splatted; it becomes a property keyed `properties`.
SELECT (1.0, 2.0)::Point AS geometry, 'foo'::String AS properties FORMAT GeoJSON;

-- No property columns produce an empty object.
SELECT (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;

-- id: string, nullable (value / empty / NULL omitted), numeric (JSON number), and escaped.
SELECT 'x' AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT CAST('y' AS Nullable(String)) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT CAST('' AS Nullable(String)) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT CAST(NULL AS Nullable(String)) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT 42::UInt64 AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT 'a"b' AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
