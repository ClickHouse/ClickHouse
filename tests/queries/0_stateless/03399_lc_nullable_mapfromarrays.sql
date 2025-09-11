-- Original reproducer from the issue
-- https://github.com/ClickHouse/ClickHouse/issues/77803
SELECT mapFromArrays(
    [toNullable(toLowCardinality('c')), toFixedString(toFixedString('d', toUInt256(1)), toLowCardinality(1))],
    map('b', 1, toFixedString('a', 1), 2)
)
GROUP BY 1;

SELECT '';
SELECT '-- Literal tests';

-- Simpler test variations
SELECT mapFromArrays([toLowCardinality(toNullable('a')), toLowCardinality(toNullable('b'))], [1, 2]) GROUP BY 1;
SELECT mapFromArrays([toLowCardinality(toNullable(1)), toLowCardinality(toNullable(2))], [3, 4]) GROUP BY 1;

SELECT mapFromArrays(
    [toLowCardinality(toNullable(1)), toLowCardinality(cast(NULL as Nullable(Int32)))],
    [3, 4]
) GROUP BY 1; -- { serverError BAD_ARGUMENTS }

SELECT mapFromArrays(
    [toLowCardinality(toNullable('x')), toLowCardinality(cast(NULL as Nullable(String)))],
    [3, 4]
) GROUP BY 1; -- { serverError BAD_ARGUMENTS }

SELECT '';
SELECT '-- Table tests';

-- Run tests on tables
SET allow_suspicious_low_cardinality_types=1;

DROP TABLE IF EXISTS 03399_lc_nullable_int_simple;
CREATE TABLE 03399_lc_nullable_int_simple(
    k Array(LowCardinality(Nullable(Int32))),
    v Array(Int32)
) engine = Memory
AS
SELECT [1, 2], [3, 4];

SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_int_simple;
SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_int_simple GROUP BY 1;

DROP TABLE IF EXISTS 03399_lc_nullable_int_simple;

DROP TABLE IF EXISTS 03399_lc_nullable_int_mixed;
CREATE TABLE 03399_lc_nullable_int_mixed(
    k Array(LowCardinality(Nullable(Int32))),
    v Array(Int32)
) engine = Memory
AS
SELECT [1, 2], [3, 4]
UNION ALL
SELECT [5, null], [7, 8];

SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_int_mixed; -- { serverError BAD_ARGUMENTS }
SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_int_mixed GROUP BY 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS 03399_lc_nullable_int_mixed;


DROP TABLE IF EXISTS 03399_lc_nullable_string_simple;
CREATE TABLE 03399_lc_nullable_string_simple(
    k Array(LowCardinality(Nullable(String))),
    v Array(Int32)
) engine = Memory
AS
SELECT ['a', 'b'], [1, 2];

SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_string_simple;
SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_string_simple GROUP BY 1;

DROP TABLE IF EXISTS 03399_lc_nullable_string_simple;

DROP TABLE IF EXISTS 03399_lc_nullable_string_mixed;
CREATE TABLE 03399_lc_nullable_string_mixed(
    k Array(LowCardinality(Nullable(String))),
    v Array(Int32)
) engine = Memory
AS
SELECT ['a', 'b'], [1, 2]
UNION ALL
SELECT [NULL, 'c'], [3, 4];

SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_string_mixed; -- { serverError BAD_ARGUMENTS }
SELECT mapFromArrays(k, v) FROM 03399_lc_nullable_string_mixed GROUP BY 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS 03399_lc_nullable_string_mixed;
