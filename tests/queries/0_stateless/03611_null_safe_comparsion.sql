SET enable_analyzer = 1;
SELECT '=====================================================================';
SELECT 'Test : ClickHouse NULL-safe comparison';
SELECT '(1) <=> (IS NOT DISTINCT FROM)';
SELECT '(2) IS DISTINCT FROM';
SELECT '=====================================================================';
SELECT 'Purpose:';
SELECT '1. Validate behavior of <=> and IS DISTINCT FROM across a wide range of ClickHouse data types and SQL clauses.';
SELECT '2. Cover numeric, floating, string, enum, date/time, complex types, and NULL / NaN edge cases.';
SELECT '3. test null-safe comparison in SELECT clause, WHERE, ORDER BY, GROUP BY, HAVING, JOIN, CASE/IF, WINDOW, and subqueries.';
SELECT '=====================================================================';

SELECT '0. Main table with many types';
DROP TABLE IF EXISTS 03611_nscmp_tbl;
CREATE TABLE 03611_nscmp_tbl
(
    `key` Int64,
    `c_int8` Nullable(Int8),
    `c_int16` Nullable(Int16),
    `c_int32` Nullable(Int32),
    `c_int64` Nullable(Int64),
    `c_uint8` Nullable(UInt8),
    `c_uint16` Nullable(UInt16),
    `c_uint32` Nullable(UInt32),
    `c_uint64` Nullable(UInt64),
    `c_float32` Nullable(Float32),
    `c_float64` Nullable(Float64),
    `c_decimal` Nullable(Decimal(18, 4)),
    `c_date` Nullable(Date),
    `c_datetime` Nullable(DateTime),
    `c_dt64` Nullable(DateTime64(3)),
    `c_string` Nullable(String),
    `c_fstring` Nullable(FixedString(4)),
    `c_enum8` Nullable(Enum8('a' = 1, 'b' = 2, '' = 0)),
    `c_enum16` Nullable(Enum16('x' = 100, 'y' = 200, '' = 0)),
    `c_array` Array(Nullable(Int32)),
    `c_tuple` Tuple(Nullable(Int32),Nullable(String)),
    `c_map` Map(String, Nullable(Int32)),
    `c_nullable` Nullable(Int32),
    `c_uuid` Nullable(UUID),
    `c_ipv4` Nullable(IPv4),
    `c_ipv6` Nullable(IPv6),
    `c_json` Nullable(JSON),
    `c_nested` Nested(
        id Nullable(Int32),
        value Nullable(String)
    ),
    `c_variant` Variant(UInt64, String, Array(UInt64)),
    `c_dynamic` Dynamic
)
ENGINE = MergeTree
ORDER BY key;

SELECT 'Insert rows containing:';
SELECT '• normal values';
SELECT '• NULLs';
SELECT '• NaN';
SELECT '• edge numeric boundaries';

INSERT INTO 03611_nscmp_tbl VALUES
(
    1,
    1,1,1,1, 1,1,1,1, 1.0,1.0, 123.4567,
    '2025-09-22', '2025-09-22 12:34:56', '2025-09-22 12:34:56.789',
    'abc','abcd','a','x',
    [1,2,3],
    (1,'t'),
    map('k1',1),
    100,
    generateUUIDv4(),
    '127.0.0.1',
    '::1',
    '{"k":"v"}',
    [1],       -- c_nested.id
    ['test nested'], -- c_nested.value
    'test variant', -- c_variant
    'test dynamic'  -- c_dynamic
),
(
    2,
    NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,   -- c_nested.id NULL
    NULL,   -- c_nested.value NULL
    NULL,   -- c_variant
    NULL    -- c_dynamic
),
(
    3,
    2,2,2,2, 2,2,2,2, nan,nan, 999.9999,
    '2025-09-23', '2025-09-23 01:02:03', '2025-09-23 01:02:03.321',
    'xyz','zzzz','b','y',
    [4,5],
    (2,'u'),
    map('k2',2),
    200,
    generateUUIDv4(),
    '10.0.0.1',
    '2001:db8::1',
    '{"k2":"v2"}',
    [201],          -- c_nested.id
    ['nested_val'],  -- c_nested.value
    24, -- c_variant
    12  -- c_dynamic
),
(
    4,
    -5,-5,-5,-5, 255,65535,4294967295,18446744073709551615, -1,0.0, -123.0001,
    '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00.000',
    '', 'aaaa','a','x',
    [],(0,''),
    map('edge',-1),
    NULL,
    generateUUIDv4(),
    '0.0.0.0',
    '::',
    '{}',
    [],   -- c_nested.id empty
    [],    -- c_nested.value empty
    [1, 2, 3],  -- c_variant
    [2, 3, 4]   -- c_dynamic
);


SELECT '1.1 Basic NULL-safe equality and distinctness';
SELECT 'Compare same column to itself';
SELECT
    -- Integers
    c_int8 <=> c_int8 AS c_int8_self_eq,
    c_int8 IS DISTINCT FROM c_int8 AS c_int8_self_distinct,

    c_int16 <=> c_int16 AS c_int16_self_eq,
    c_int16 IS DISTINCT FROM c_int16 AS c_int16_self_distinct,

    c_int32 <=> c_int32 AS c_int32_self_eq,
    c_int32 IS DISTINCT FROM c_int32 AS c_int32_self_distinct,

    c_int64 <=> c_int64 AS c_int64_self_eq,
    c_int64 IS DISTINCT FROM c_int64 AS c_int64_self_distinct,

    c_uint8 <=> c_uint8 AS c_uint8_self_eq,
    c_uint8 IS DISTINCT FROM c_uint8 AS c_uint8_self_distinct,

    c_uint16 <=> c_uint16 AS c_uint16_self_eq,
    c_uint16 IS DISTINCT FROM c_uint16 AS c_uint16_self_distinct,

    c_uint32 <=> c_uint32 AS c_uint32_self_eq,
    c_uint32 IS DISTINCT FROM c_uint32 AS c_uint32_self_distinct,

    c_uint64 <=> c_uint64 AS c_uint64_self_eq,
    c_uint64 IS DISTINCT FROM c_uint64 AS c_uint64_self_distinct,

    -- Floating-point
    c_float32 <=> c_float32 AS c_float32_self_eq,
    c_float32 IS DISTINCT FROM c_float32 AS c_float32_self_distinct,

    c_float64 <=> c_float64 AS c_float64_self_eq,
    c_float64 IS DISTINCT FROM c_float64 AS c_float64_self_distinct,

    -- Decimal
    c_decimal <=> c_decimal AS c_decimal_self_eq,
    c_decimal IS DISTINCT FROM c_decimal AS c_decimal_self_distinct,

    -- Date / DateTime
    c_date <=> c_date AS c_date_self_eq,
    c_date IS DISTINCT FROM c_date AS c_date_self_distinct,

    c_datetime <=> c_datetime AS c_datetime_self_eq,
    c_datetime IS DISTINCT FROM c_datetime AS c_datetime_self_distinct,

    c_dt64 <=> c_dt64 AS c_dt64_self_eq,
    c_dt64 IS DISTINCT FROM c_dt64 AS c_dt64_self_distinct,

    -- Strings
    c_string <=> c_string AS c_string_self_eq,
    c_string IS DISTINCT FROM c_string AS c_string_self_distinct,

    c_fstring <=> c_fstring AS c_fstring_self_eq,
    c_fstring IS DISTINCT FROM c_fstring AS c_fstring_self_distinct,

    -- Enums
    c_enum8 <=> c_enum8 AS c_enum8_self_eq,
    c_enum8 IS DISTINCT FROM c_enum8 AS c_enum8_self_distinct,

    c_enum16 <=> c_enum16 AS c_enum16_self_eq,
    c_enum16 IS DISTINCT FROM c_enum16 AS c_enum16_self_distinct,

    -- Arrays
    c_array <=> c_array AS c_array_self_eq,
    c_array IS DISTINCT FROM c_array AS c_array_self_distinct,

    -- Tuple
    c_tuple <=> c_tuple AS c_tuple_self_eq,
    c_tuple IS DISTINCT FROM c_tuple AS c_tuple_self_distinct,

    -- Map
    c_map <=> c_map AS c_map_self_eq,
    c_map IS DISTINCT FROM c_map AS c_map_self_distinct,

    -- Nullable basic
    c_nullable <=> c_nullable AS c_nullable_self_eq,
    c_nullable IS DISTINCT FROM c_nullable AS c_nullable_self_distinct,

    -- UUID / IP
    c_uuid <=> c_uuid AS c_uuid_self_eq,
    c_uuid IS DISTINCT FROM c_uuid AS c_uuid_self_distinct,

    c_ipv4 <=> c_ipv4 AS c_ipv4_self_eq,
    c_ipv4 IS DISTINCT FROM c_ipv4 AS c_ipv4_self_distinct,

    c_ipv6 <=> c_ipv6 AS c_ipv6_self_eq,
    c_ipv6 IS DISTINCT FROM c_ipv6 AS c_ipv6_self_distinct,

    -- JSON
    c_json <=> c_json AS c_json_self_eq,
    c_json IS DISTINCT FROM c_json AS c_json_self_distinct,

    -- Nested
    c_nested.id <=> c_nested.id AS c_nested_id_self_eq,
    c_nested.id IS DISTINCT FROM c_nested.id AS c_nested_id_self_distinct,
    c_nested.value <=> c_nested.value AS c_nested_value_self_eq,
    c_nested.value IS DISTINCT FROM c_nested.value AS c_nested_value_self_distinct,

    -- Variant
    c_variant <=> c_variant AS c_variant_self_eq,
    c_variant IS DISTINCT FROM c_variant AS c_variant_self_distinct,

    -- Dynamic
    c_dynamic <=> c_dynamic AS c_dynamic_self_eq,
    c_dynamic IS DISTINCT FROM c_dynamic AS c_dynamic_self_distinct

FROM 03611_nscmp_tbl
ORDER BY key;


SELECT '1.2 NaN behavior';
SELECT 'NaN <=> NaN = 0, NaN <=> number = 0';
SELECT 'NaN is distinct from NaN = 1, NaN is distinct from number = 1';
SELECT
    c_float32,
    c_float32 <=> nan AS nan_cmp,
    c_float32 IS DISTINCT FROM nan AS nan_distinct,
    c_float32 <=> 1.0 AS cmp_with_one,
    c_float32 IS DISTINCT FROM 1.0 AS cmp_with_one_distinct
FROM 03611_nscmp_tbl
ORDER BY key;

-- 1.3 String, FixedString, Enum
SELECT '1.3 String, FixedString, Enum';
SELECT
    c_string <=> 'abc' AS string_vs_literal,
    c_string IS DISTINCT FROM 'abc' AS string_vs_literal_distinct,
    c_string <=> NULL AS string_vs_null,
    c_string IS DISTINCT FROM NULL AS string_vs_null_distinct,

    c_fstring <=> 'abcd' AS fstring_vs_literal,
    c_fstring IS DISTINCT FROM 'abcd' AS fstring_vs_literal_distinct,
    c_fstring <=> NULL AS fstring_vs_null,
    c_fstring IS DISTINCT FROM NULL AS fstring_vs_null_distinct,

    c_enum8 <=> CAST('a' AS Enum8('a'=1,'b'=2)) AS enum8_check,
    c_enum8 IS DISTINCT FROM CAST('a' AS Enum8('a'=1,'b'=2)) AS enum8_check_distinct,
    c_enum8 <=> NULL AS enum8_vs_null,
    c_enum8 IS DISTINCT FROM NULL AS enum8_vs_null_distinct,

    c_enum16 <=> CAST('x' AS Enum16('x'=100,'y'=200)) AS enum16_check,
    c_enum16 IS DISTINCT FROM CAST('x' AS Enum16('x'=100,'y'=200)) AS enum16_check_distinct,
    c_enum16 <=> NULL AS enum16_vs_null,
    c_enum16 IS DISTINCT FROM NULL AS enum16_vs_null_distinct
FROM 03611_nscmp_tbl
ORDER BY key;

-- 1.4 Date and time
SELECT '1.4 Date and time';
SELECT
    c_date <=> toDate('2025-09-22') AS date_check,
    c_date IS DISTINCT FROM toDate('2025-09-22') AS date_check_distinct,
    c_date <=> NULL AS date_vs_null,
    NULL <=> c_date AS null_vs_date,
    c_date IS DISTINCT FROM NULL AS date_vs_null_distinct,
    NULL IS DISTINCT FROM c_date AS null_vs_date_distinct,

    c_datetime <=> toDateTime('2025-09-22 12:34:56') AS datetime_check,
    c_datetime IS DISTINCT FROM toDateTime('2025-09-22 12:34:56') AS datetime_check_distinct,
    c_datetime <=> NULL AS datetime_vs_null,
    NULL <=> c_datetime AS null_vs_datetime,
    c_datetime IS DISTINCT FROM NULL AS datetime_vs_null_distinct,
    NULL IS DISTINCT FROM c_datetime AS null_vs_datetime_distinct,

    c_dt64 <=> toDateTime64('2025-09-22 12:34:56.789',3) AS dt64_check,
    c_dt64 IS DISTINCT FROM toDateTime64('2025-09-22 12:34:56.789',3) AS dt64_check_distinct,
    c_dt64 <=> NULL AS dt64_vs_null,
    NULL <=> c_dt64 AS null_vs_c_dt64,
    c_dt64 IS DISTINCT FROM NULL AS dt64_vs_null_distinct,
    NULL IS DISTINCT FROM c_dt64 AS null_vs_dt64_distinct
FROM 03611_nscmp_tbl
ORDER BY key;

-- 1.5 Complex types: Array / Tuple / Map / Nullable / Variant / Dynamic / JSON
SELECT '1.5 Complex types: Array / Tuple / Map / Variant / Dynamic / JSON';

SELECT
    -- =====================
    -- Array cmp
    -- =====================
    c_array <=> [1, 2, 3] AS array_check,
    c_array IS DISTINCT FROM [1, 2, 3] AS array_check_distinct,
    c_array <=> [1, 2, NULL] AS array_check_with_null,
    c_array IS DISTINCT FROM [1, 2, NULL] AS array_check_with_null_distinct,
    c_array <=> [] AS array_empty_check,
    c_array IS DISTINCT FROM [] AS array_empty_check_distinct,
    c_array <=> NULL AS array_vs_null,
    NULL <=> c_array AS null_vs_array,
    c_array IS DISTINCT FROM NULL AS array_vs_null_distinct,
    NULL IS DISTINCT FROM c_array AS null_vs_array_distinct,

    -- =====================
    -- Tuple cmp
    -- =====================
    c_tuple <=> (1, 't') AS tuple_check,
    c_tuple IS DISTINCT FROM (1, 't') AS tuple_check_distinct,
    c_tuple <=> (NULL, NULL) AS tuple_null_check,
    c_tuple IS DISTINCT FROM (NULL, NULL) AS tuple_null_check_distinct,

    -- =====================
    -- Map cmp
    -- =====================
    c_map <=> map('k1', 1) AS map_check,
    c_map IS DISTINCT FROM map('k1', 1) AS map_check_distinct,
    c_map <=> map('k1', NULL, 'k2', 2) AS map_check_with_null,
    c_map IS DISTINCT FROM map('k1', NULL, 'k2', 2) AS map_check_with_null_distinct,
    c_map <=> map() AS map_empty_check,
    c_map IS DISTINCT FROM map() AS map_empty_check_distinct,
    c_map <=> NULL AS map_vs_null,
    NULL <=> c_map AS null_vs_map,
    c_map IS DISTINCT FROM NULL AS map_vs_null_distinct,
    NULL IS DISTINCT FROM c_map AS null_vs_map_distinct,

    -- =====================
    -- Nullable cmp
    -- =====================
    c_nullable <=> 100 AS nullable_vs_literal,
    c_nullable IS DISTINCT FROM 100 AS nullable_vs_literal_distinct,
    c_nullable <=> NULL AS nullable_vs_null,
    NULL <=> c_nullable AS null_vs_nullable,
    c_nullable IS DISTINCT FROM NULL AS nullable_vs_null_distinct,
    NULL IS DISTINCT FROM c_nullable AS null_vs_nullable_distinct,

    -- =====================
    -- JSON cmp
    -- =====================
    c_json <=> CAST('{"k": "v"}' AS JSON) AS json_check,
    c_json IS DISTINCT FROM CAST('{"k": "v"}' AS JSON) AS json_check_distinct,
    c_json <=> CAST('{"k2": "v2"}' AS JSON) AS json_check2,
    c_json IS DISTINCT FROM CAST('{"k2": "v2"}' AS JSON) AS json_check2_distinct,
    c_json <=> CAST('{}' AS JSON) AS json_empty_check,
    c_json IS DISTINCT FROM CAST('{}' AS JSON) AS json_empty_check_distinct,
    c_json <=> NULL AS json_vs_null,
    NULL <=> c_json AS null_vs_json,
    c_json IS DISTINCT FROM NULL AS json_vs_null_distinct,
    NULL IS DISTINCT FROM c_json AS null_vs_json_distinct,

    -- =====================
    -- Variant cmp
    -- =====================
    c_variant <=> 1::UInt64::Variant(UInt64, String, Array(UInt64)) AS variant_check_int,
    c_variant IS DISTINCT FROM 1::UInt64::Variant(UInt64, String, Array(UInt64)) AS variant_check_int_distinct,
    c_variant <=> 'test variant'::String::Variant(UInt64, String, Array(UInt64)) AS variant_check_str,
    c_variant IS DISTINCT FROM 'test variant'::String::Variant(UInt64, String, Array(UInt64)) AS variant_check_str_distinct,
    c_variant <=> [1,2,3]::Array(UInt64)::Variant(UInt64, String, Array(UInt64)) AS variant_check_array,
    c_variant IS DISTINCT FROM [1,2,3]::Array(UInt64)::Variant(UInt64, String, Array(UInt64)) AS variant_check_array_distinct,
    c_variant <=> NULL AS variant_vs_null,
    NULL <=> c_variant AS null_vs_variant,
    c_variant IS DISTINCT FROM NULL AS variant_vs_null_distinct,
    NULL IS DISTINCT FROM c_variant AS null_vs_variant_distinct,

    -- =====================
    -- Dynamic cmp
    -- =====================
    c_dynamic <=> 1::Dynamic AS dynamic_check_int,
    c_dynamic IS DISTINCT FROM 1::Dynamic AS dynamic_check_int_distinct,
    c_dynamic <=> 'test dynamic'::Dynamic AS dynamic_check_str,
    c_dynamic IS DISTINCT FROM 'test dynamic'::Dynamic AS dynamic_check_str_distinct,
    c_dynamic <=> [1, 2, 3]::Dynamic AS dynamic_check_array,
    c_dynamic IS DISTINCT FROM [1, 2, 3]::Dynamic AS dynamic_check_array_distinct,
    c_dynamic <=> NULL AS dynamic_vs_null,
    NULL <=> c_dynamic AS null_vs_dynamic,
    c_dynamic IS DISTINCT FROM NULL AS dynamic_vs_null_distinct,
    NULL IS DISTINCT FROM c_dynamic AS null_vs_dynamic_distinct

FROM 03611_nscmp_tbl
ORDER BY key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;

-- 1.6 UUID / IPv4 / IPv6
SELECT '1.6 UUID / IPv4 / IPv6 comparisons';
SELECT 'NULL-safe comparisons with type-correct literals';
SELECT
    c_uuid <=> c_uuid AS uuid_self,
    c_uuid IS DISTINCT FROM c_uuid AS uuid_self_distinct,
    c_uuid <=> NULL AS uuid_vs_null,
    NULL <=> c_uuid AS null_vs_uuid,
    c_uuid IS DISTINCT FROM NULL AS uuid_vs_null_distinct,
    NULL IS DISTINCT FROM c_uuid AS null_vs_uuid_distinct,

    c_ipv4 <=> toIPv4('127.0.0.1') AS ipv4_check,
    c_ipv4 IS DISTINCT FROM toIPv4('127.0.0.1') AS ipv4_check_distinct,
    c_ipv4 <=> NULL AS ipv4_vs_null,
    NULL <=> c_ipv4 AS null_vs_ipv4,
    c_ipv4 IS DISTINCT FROM NULL AS ipv4_vs_null_distinct,
    NULL IS DISTINCT FROM c_ipv4 AS null_vs_ipv4_distinct,

    c_ipv6 <=> toIPv6('::1') AS ipv6_check,
    c_ipv6 IS DISTINCT FROM toIPv6('::1') AS ipv6_check_distinct,
    c_ipv6 <=> NULL AS ipv6_vs_null,
    NULL <=> c_ipv6 AS null_vs_ipv6,
    c_ipv6 IS DISTINCT FROM NULL AS ipv6_vs_null_distinct,
    NULL IS DISTINCT FROM c_ipv6 AS null_vs_ipv6_distinct
FROM 03611_nscmp_tbl
ORDER BY key;

SELECT '1.7 Cross-type test cases';
-- Variant vs Dynamic
SELECT
    c_variant <=> c_dynamic,
    c_variant IS DISTINCT FROM c_dynamic
FROM 03611_nscmp_tbl
ORDER BY key;

-- Dynamic vs IPv4/IPv6
SELECT c_dynamic <=> c_ipv4 FROM 03611_nscmp_tbl ORDER BY key;

SELECT c_dynamic IS DISTINCT FROM c_ipv4 FROM 03611_nscmp_tbl ORDER BY key;

SELECT c_dynamic <=> c_ipv6 FROM 03611_nscmp_tbl ORDER BY key;

SELECT c_dynamic IS DISTINCT FROM c_ipv6 FROM 03611_nscmp_tbl ORDER BY key;

-- Dynamic vs String
SELECT
    c_dynamic <=> c_string,
    c_dynamic <=> c_fstring
FROM 03611_nscmp_tbl
ORDER BY key;

SELECT
    c_dynamic IS DISTINCT FROM c_string,
    c_dynamic IS DISTINCT FROM c_fstring
FROM 03611_nscmp_tbl
ORDER BY key;

-- Array vs Dynamic
SELECT
    c_array <=> c_dynamic,
    c_array IS DISTINCT FROM c_dynamic
FROM 03611_nscmp_tbl
ORDER BY key;

-- Map vs Dynamic
SELECT
    c_map <=> c_dynamic,
    c_map IS DISTINCT FROM c_dynamic
FROM 03611_nscmp_tbl
ORDER BY key;

-- Nested vs Dynamic
SELECT
    c_nested.id <=> c_dynamic,
    c_nested.value <=> c_dynamic
FROM 03611_nscmp_tbl
ORDER BY key;

-- Tuple vs Dynamic

SELECT
    c_tuple <=> c_dynamic,
    c_tuple IS DISTINCT FROM c_dynamic
FROM 03611_nscmp_tbl
ORDER BY key;


SELECT '1.8 Some ILLEGAL_TYPE_OF_ARGUMENT case';
SELECT
    c_tuple <=> NULL AS tuple_vs_null
FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    NULL <=> c_tuple AS tuple_vs_null
FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    c_tuple IS DISTINCT FROM NULL AS tuple_vs_null_distinct
FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    NULL IS DISTINCT FROM c_tuple AS tuple_vs_null_distinct
FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT c_variant = 1 FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT c_variant <=> c_ipv4 FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT c_variant IS DISTINCT FROM c_ipv4 FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT c_variant <=> c_ipv6 FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT c_variant IS DISTINCT FROM c_ipv6 FROM 03611_nscmp_tbl; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    c_variant <=> c_string,
    c_variant <=> c_fstring
FROM 03611_nscmp_tbl;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT
    c_variant IS DISTINCT FROM c_string,
    c_variant IS DISTINCT FROM c_fstring
FROM 03611_nscmp_tbl;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Array vs Map cross-type test cases';

-- Array vs Map
SELECT
    c_array <=> c_map,
    c_array IS DISTINCT FROM c_map
FROM 03611_nscmp_tbl;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Map vs Variant cross-type test cases';

-- Map vs Variant
SELECT
    c_map <=> c_variant,
    c_map IS DISTINCT FROM c_variant
FROM 03611_nscmp_tbl;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Array vs Variant
SELECT
    c_array <=> c_variant,
    c_array IS DISTINCT FROM c_variant
FROM 03611_nscmp_tbl;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


-- Array vs Tuple
SELECT
    c_array <=> c_tuple,
    c_array IS DISTINCT FROM c_tuple
FROM 03611_nscmp_tbl;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Map vs Tuple
SELECT
    c_map <=> c_tuple,
    c_map IS DISTINCT FROM c_tuple
FROM 03611_nscmp_tbl;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS 03611_t_nullsafe;
CREATE TABLE IF NOT EXISTS 03611_t_nullsafe
(
    id Int32,
    a Nullable(Int32),
    b Nullable(Int32),
    txt Nullable(String)
) ENGINE = Memory;

INSERT INTO 03611_t_nullsafe VALUES
(1, 1, 1, 'x'),
(2, 1, NULL, 'x'),
(3, NULL, NULL, 'y'),
(4, 2, 2, 'z');

SELECT '1.9 WHERE Clause';
SELECT id, a, b,
       (a <=> b) AS null_safe_equal,
       (a IS DISTINCT FROM b) AS null_safe_distinct
FROM 03611_t_nullsafe
ORDER BY id;


SELECT '1.10 ORDER BY Clause';
SELECT id, a, b,
       (a IS DISTINCT FROM b) AS distinct_flag
FROM 03611_t_nullsafe
ORDER BY (a <=> b) DESC, distinct_flag ASC, id;

SELECT '1.11 GROUP BY Clause';
SELECT
    a <=> b AS eq,
    a IS DISTINCT FROM b AS distinct_flag,
    count() AS cnt,
    groupArray(id) AS ids
FROM 03611_t_nullsafe
GROUP BY eq, distinct_flag
ORDER BY eq;

SELECT '1.12 Aggregate Func';
SELECT
    countIf(a <=> 1) AS cnt_equal_1,
    countIf(a IS DISTINCT FROM 1) AS cnt_distinct_1,
    count() AS total_count
FROM 03611_t_nullsafe
ORDER BY cnt_equal_1;

SELECT '1.12 HAVING Clause';
SELECT
    max(a) AS max_a,
    max(b) AS max_b
FROM `03611_t_nullsafe`
HAVING (max_a <=> max_b) OR (max_a IS DISTINCT FROM max_b)
ORDER BY max_a;

SELECT '1.13 JOIN Clause';
SELECT
    l.id AS lid,
    r.id AS rid,
    l.a,
    r.b
FROM 03611_t_nullsafe AS l
INNER JOIN 03611_t_nullsafe AS r ON l.a <=> r.b
ORDER BY lid, rid;

SELECT
    l.id AS lid,
    r.id AS rid,
    l.a,
    r.b
FROM 03611_t_nullsafe AS l
INNER JOIN 03611_t_nullsafe AS r ON l.a IS DISTINCT FROM r.b
ORDER BY lid, rid;

SELECT '1.14 CASE WHEN Clause';
SELECT id, a, b,
       CASE
           WHEN a <=> b THEN 'null_safe_equal'
           WHEN a IS DISTINCT FROM b THEN 'null_safe_distinct'
           ELSE 'unexpected'
       END AS comparison_result,
       CASE
           WHEN a <=> b THEN 'same'
           WHEN a IS DISTINCT FROM b THEN 'different'
           ELSE 'unknown'
       END AS status
FROM 03611_t_nullsafe
ORDER BY id;

SELECT '1.15 IF function';
SELECT id, a, b,
       IF(a <=> b, 'safe-equal', 'not_equal') AS equal_flag,
       IF(a IS DISTINCT FROM b, 'distinct', 'same') AS distinct_flag
FROM 03611_t_nullsafe
ORDER BY id;

SELECT '1.16 Window Func';
SELECT (a IS DISTINCT FROM b) AS distinct_flag,
       count() OVER (PARTITION BY a <=> b) AS partition_by_eq,
       count() OVER (PARTITION BY a IS DISTINCT FROM b) AS partition_by_distinct,
       count() OVER (PARTITION BY a <=> b, a IS DISTINCT FROM b) AS partition_by_both
FROM 03611_t_nullsafe
ORDER BY distinct_flag;

SELECT '1.17 OR / AND';
select 1 <=> 1 AND 1 is DISTINCT FROM 1,
       1 <=> 1 OR 1 is DISTINCT FROM 1;

SELECT '1.18 Tuple has null';
select (null,null,null) is DISTINCT FROM (null,null,null);
select (null,null,null) is DISTINCT FROM (null,null,1);
select (null,null,null) <=> (null,null,1);
select (null,null,null) <=> (null,null,null);

select (null,null,(null,null,null)) is DISTINCT FROM (null,null,(null,null,1));
select (null,null,(null,null,null)) is DISTINCT FROM (null,null,(null,null,null));
select (null,null,(null,null,null)) <=> (null,null,(null,null,1));
select (null,null,(null,null,null)) <=> (null,null,(null,null,null));

SELECT DISTINCT * WHERE 2 <=> materialize(2)
GROUP BY 1
WITH TOTALS QUALIFY ((2 <=> 2) / ((2 IS NOT NULL) IS NULL), *, *, materialize(toNullable(2)), materialize(2), materialize(materialize(2)), 2)
<=> ((2 = *) * 2, *, *, 2, toNullable(2), 2, 2);

SELECT '1.19 Alias function testing';
SELECT isNotDistinctFrom(1, 1);
SELECT isNotDistinctFrom(1, 2);
SELECT isNotDistinctFrom(1, null);
SELECT isNotDistinctFrom(null, null);

SELECT isDistinctFrom(1, 1);
SELECT isDistinctFrom(1, 2);
SELECT isDistinctFrom(1, null);
SELECT isDistinctFrom(null, null);

DROP TABLE IF EXISTS 03611_nscmp_tbl;
DROP TABLE IF EXISTS 03611_t_nullsafe;
