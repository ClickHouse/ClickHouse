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
    `c_int8` Int8,
    `c_int16` Int16,
    `c_int32` Int32,
    `c_int64` Int64,
    `c_uint8` UInt8,
    `c_uint16` UInt16,
    `c_uint32` UInt32,
    `c_uint64` UInt64,
    `c_float32` Float32,
    `c_float64` Float64,
    `c_decimal` Decimal(18, 4),
    `c_date` Date,
    `c_datetime` DateTime,
    `c_dt64` DateTime64(3),
    `c_string` String,
    `c_fstring` FixedString(4),
    `c_enum8` Enum8('a' = 1, 'b' = 2, '' = 0),
    `c_enum16` Enum16('x' = 100, 'y' = 200, '' = 0),
    `c_array` Array(Int32),
    `c_tuple` Tuple(Int32,String),
    `c_map` Map(String, Int32),
    `c_nullable` Nullable(Int32),
    `c_uuid` UUID,
    `c_ipv4` IPv4,
    `c_ipv6` IPv6,
    `c_json` JSON
)
ENGINE = MergeTree
ORDER BY c_int32;

SELECT 'Insert rows containing:';
SELECT '• normal values';
SELECT '• NULLs';
SELECT '• NaN';
SELECT '• edge numeric boundaries';
INSERT INTO 03611_nscmp_tbl VALUES
(
    -- Normal row with all valid values
    1,1,1,1,
    1,1,1,1,
    1.0,1.0,
    123.4567,
    '2025-09-22',
    '2025-09-22 12:34:56',
    '2025-09-22 12:34:56.789',
    'abc','abcd','a','x',
    [1,2,3],
    (1,'t'),
    map('k1',1),
    100,
    generateUUIDv4(),
    '127.0.0.1',
    '::1',
    '{"k":"v"}'
),
(
    -- Row with all NULLs to test NULL-safe comparisons
    NULL,NULL,NULL,NULL,
    NULL,NULL,NULL,NULL,
    NULL,NULL,
    NULL,
    NULL,NULL,NULL,
    NULL,NULL,
    NULL,NULL,
    [],(NULL,NULL),map(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
),
(
    2,2,2,2,
    2,2,2,2,
    nan,
    nan,
    999.9999,
    '2025-09-23',
    '2025-09-23 01:02:03',
    '2025-09-23 01:02:03.321',
    'xyz','zzzz','b','y',
    [4,5],
    (2,'u'),
    map('k2',2),
    200,
    generateUUIDv4(),
    '10.0.0.1',
    '2001:db8::1',
    '{"k2":"v2"}'
),
(
    -5,-5,-5,-5,
    255,65535,4294967295,18446744073709551615,
    -1,0.0,
    -123.0001,
    '1970-01-01',
    '1970-01-01 00:00:00',
    '1970-01-01 00:00:00.000',
    '', 'aaaa','a','x',
    [],(0,''),
    map('edge',-1),
    NULL,
    generateUUIDv4(),
    '0.0.0.0',
    '::',
    '{}'
);

SELECT '1.1 Basic NULL-safe equality and distinctness';
SELECT 'Compare same column to itself and cross type';
SELECT
    c_int8 <=> c_int8      AS int8_num_self,
    c_int8 IS DISTINCT FROM c_int8 AS int8_num_self_distinct,
    c_float32 <=> c_float32 AS float_self_cmp,
    c_float32 IS DISTINCT FROM c_float32 AS float_self_distinct,
    c_nullable <=> NULL AS nullable_vs_null,
    c_nullable IS DISTINCT FROM NULL AS nullable_vs_null_distinct,
    c_float32 <=> c_float64 AS float_cross,
    c_float32 IS DISTINCT FROM c_float64 AS float_cross_distinct
FROM 03611_nscmp_tbl
ORDER BY c_int32;

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
ORDER BY c_int32;

SELECT '1.3 String, FixedString, Enum';
SELECT
    c_string <=> 'abc' AS string_vs_literal,
    c_string IS DISTINCT FROM 'abc' AS string_vs_literal_distinct,
    c_fstring <=> 'abcd' AS fstring_vs_literal,
    c_fstring IS DISTINCT FROM 'abcd' AS fstring_vs_literal_distinct,
    c_enum8 <=> CAST('a' AS Enum8('a'=1,'b'=2)) AS enum8_check,
    c_enum8 IS DISTINCT FROM CAST('a' AS Enum8('a'=1,'b'=2)) AS enum8_check_distinct,
    c_enum16 <=> CAST('x' AS Enum16('x'=100,'y'=200)) AS enum16_check,
    c_enum16 IS DISTINCT FROM CAST('x' AS Enum16('x'=100,'y'=200)) AS enum16_check_distinct
FROM 03611_nscmp_tbl
ORDER BY c_int32;

SELECT '1.4 Date and time';
SELECT
    c_date <=> toDate('2025-09-22') AS date_check,
    c_date IS DISTINCT FROM toDate('2025-09-22') AS date_check_distinct,
    c_datetime <=> toDateTime('2025-09-22 12:34:56') AS datetime_check,
    c_datetime IS DISTINCT FROM toDateTime('2025-09-22 12:34:56') AS datetime_check_distinct,
    c_dt64 <=> toDateTime64('2025-09-22 12:34:56.789',3) AS dt64_check,
    c_dt64 IS DISTINCT FROM toDateTime64('2025-09-22 12:34:56.789',3) AS dt64_check_distinct
FROM 03611_nscmp_tbl
ORDER BY c_int32;

SELECT '1.5 Complex types: Array / Tuple / Map / JSON';
SELECT
    c_array <=> [1,2,3] AS array_check,
    c_array IS DISTINCT FROM [1,2,3] AS array_check_distinct,
    c_tuple <=> (1,'t') AS tuple_check,
    c_tuple IS DISTINCT FROM (1,'t') AS tuple_check_distinct,
    c_map <=> map('k1',1) AS map_check,
    c_map IS DISTINCT FROM map('k1',1) AS map_check_distinct,
    c_nullable <=> 100 AS nullable_vs_literal,
    c_nullable IS DISTINCT FROM 100 AS nullable_vs_literal_distinct
FROM 03611_nscmp_tbl
ORDER BY c_int32;

SELECT '1.6 UUID / IPv4 / IPv6 comparisons';
SELECT 'NULL-safe comparisons with type-correct literals';
SELECT
    c_uuid <=> c_uuid AS uuid_self,
    c_uuid IS DISTINCT FROM c_uuid AS uuid_self_distinct,
    c_ipv4 <=> toIPv4('127.0.0.1') AS ipv4_check,
    c_ipv4 IS DISTINCT FROM toIPv4('127.0.0.1') AS ipv4_check_distinct,
    c_ipv6 <=> toIPv6('::1') AS ipv6_check,
    c_ipv6 IS DISTINCT FROM toIPv6('::1') AS ipv6_check_distinct
FROM 03611_nscmp_tbl
ORDER BY c_int32;

SELECT 'prepare table';
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

SELECT '1.7 WHERE Clause';
SELECT id, a, b,
       (a <=> b) AS null_safe_equal,
       (a IS DISTINCT FROM b) AS null_safe_distinct
FROM 03611_t_nullsafe
ORDER BY id;

SELECT '1.8 WHERE Clause + Union All';
SELECT *
FROM
(
    SELECT
        '<=> 1' AS condition_type,
        *
    FROM `03611_t_nullsafe`
    WHERE a <=> 1
    UNION ALL
    SELECT
        'IS DISTINCT FROM 1' AS condition_type,
        *
    FROM `03611_t_nullsafe`
    WHERE a IS DISTINCT FROM 1
)
ORDER BY
    condition_type ASC,
    id ASC;


SELECT '1.9 ORDER BY Clause';
SELECT id, a, b,
       (a <=> b) AS eq,
       (a IS DISTINCT FROM b) AS distinct_flag
FROM 03611_t_nullsafe
ORDER BY eq DESC, distinct_flag ASC, id;

SELECT '1.10 GROUP BY Clause';
SELECT
    a <=> b AS eq,
    a IS DISTINCT FROM b AS distinct_flag,
    count() AS cnt,
    groupArray(id) AS ids
FROM 03611_t_nullsafe
GROUP BY eq, distinct_flag
ORDER BY eq, distinct_flag;

SELECT '1.11 Aggregate Func';
SELECT
    countIf(a <=> 1) AS cnt_equal_1,
    countIf(a IS DISTINCT FROM 1) AS cnt_distinct_1,
    count() AS total_count
FROM 03611_t_nullsafe;

SELECT '1.12 HAVING Clause';
SELECT txt,
       max(a) AS max_a,
       max(b) AS max_b,
       (max(a) <=> max(b)) AS max_equal,
       (max(a) IS DISTINCT FROM max(b)) AS max_distinct
FROM 03611_t_nullsafe
GROUP BY txt
HAVING max(a) <=> max(b) OR max(a) IS DISTINCT FROM max(b)
ORDER BY txt;

SELECT '1.13 JOIN Clause';
SELECT
    '<=> JOIN' as join_type,
    l.id AS lid,
    r.id AS rid,
    l.a,
    r.b
FROM 03611_t_nullsafe AS l
INNER JOIN 03611_t_nullsafe AS r ON l.a <=> r.b

UNION ALL

SELECT
    'IS DISTINCT FROM JOIN' as join_type,
    l.id AS lid,
    r.id AS rid,
    l.a,
    r.b
FROM 03611_t_nullsafe AS l
INNER JOIN 03611_t_nullsafe AS r ON l.a IS DISTINCT FROM r.b
ORDER BY join_type, lid, rid;

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
SELECT id, a, b,
       (a <=> b) AS eq,
       (a IS DISTINCT FROM b) AS distinct_flag,
       count() OVER (PARTITION BY a <=> b) AS partition_by_eq,
       count() OVER (PARTITION BY a IS DISTINCT FROM b) AS partition_by_distinct,
       count() OVER (PARTITION BY a <=> b, a IS DISTINCT FROM b) AS partition_by_both
FROM 03611_t_nullsafe
ORDER BY id;

SELECT '1.17 Subquery filter';
SELECT id, a, b,
       EXISTS(SELECT 1 FROM 03611_t_nullsafe s WHERE t.a <=> s.b AND s.id <> t.id) AS has_equal_match,
       EXISTS(SELECT 1 FROM 03611_t_nullsafe s WHERE t.a IS DISTINCT FROM s.b AND s.id <> t.id) AS has_distinct_match
FROM 03611_t_nullsafe t
ORDER BY id;

SELECT '1.18 OR / AND';
select 1 <=> 1 AND 1 is distinct from 1,
       1 <=> 1 OR 1 is distinct from 1;

DROP TABLE IF EXISTS 03611_nscmp_tbl;
DROP TABLE IF EXISTS 03611_t_nullsafe;
