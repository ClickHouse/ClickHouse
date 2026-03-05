-- Tests for correctness of the ordered_group_by_limit_pushdown optimization.
-- Part 4: composite (multi-column) GROUP BY keys and ORDER BY prefix matching.

-- Tags: no-parallel-replicas, long

SET ordered_group_by_limit_pushdown = 1;

DROP TABLE IF EXISTS t_gbylimit_comp;

CREATE TABLE t_gbylimit_comp
(
    a UInt32,
    b UInt32,
    c String,
    d Nullable(UInt32),
    val UInt64
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_gbylimit_comp
SELECT
    (number % 500)::UInt32,
    (number % 200)::UInt32,
    toString(number % 300),
    if(number % 97 = 0, NULL, (number % 400)::UInt32),
    number
FROM numbers(100000);

-- =====================
-- Composite key: GROUP BY a, b ORDER BY a, b LIMIT N
-- Exact match of ORDER BY and GROUP BY keys (two integers).
-- =====================
SELECT 'composite_two_int';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Composite key: GROUP BY a, c ORDER BY a, c LIMIT N
-- Mixed types (UInt32 + String).
-- =====================
SELECT 'composite_int_string';
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Composite key: GROUP BY a, b, c ORDER BY a, b, c LIMIT N
-- Three-column key.
-- =====================
SELECT 'composite_three_keys';
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Composite key with Nullable: GROUP BY a, d ORDER BY a, d LIMIT N
-- =====================
SELECT 'composite_nullable';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Prefix ORDER BY: GROUP BY a, b ORDER BY a LIMIT N
-- Heap tracks only `a`; all `b` values for matching `a` are kept.
-- Each `a` value has exactly 2 `b` sub-groups, so LIMIT 10 = 5 full `a` values
-- and falls exactly on a group boundary.
-- =====================
SELECT 'prefix_one_of_two';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 0
) ORDER BY a, b ASC;

-- =====================
-- Prefix ORDER BY: GROUP BY a, b, c ORDER BY a LIMIT N
-- One prefix column out of three GROUP BY keys.
-- Each `a` has 6 sub-groups, so LIMIT 12 = 2 full `a` values.
-- =====================
SELECT 'prefix_one_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12
    SETTINGS ordered_group_by_limit_pushdown = 1
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12
    SETTINGS ordered_group_by_limit_pushdown = 0
) ORDER BY a, b, c ASC;

-- =====================
-- Prefix ORDER BY: GROUP BY a, b, c ORDER BY a, b LIMIT N
-- Two prefix columns out of three GROUP BY keys.
-- Each (a, b) pair has 3 sub-groups (c values). LIMIT 12 = 4 full (a, b) pairs.
-- =====================
SELECT 'prefix_two_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12
    SETTINGS ordered_group_by_limit_pushdown = 1
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12
    SETTINGS ordered_group_by_limit_pushdown = 0
) ORDER BY a, b, c ASC;

-- =====================
-- Prefix ORDER BY with LIMIT offset: GROUP BY a, b ORDER BY a LIMIT 4, 6
-- Each `a` has 2 sub-groups. LIMIT 4, 6 = skip 4 (a=0,1) take 6 (a=2,3,4).
-- Both offset and limit align with `a` boundaries.
-- =====================
SELECT 'prefix_with_offset';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6
    SETTINGS ordered_group_by_limit_pushdown = 1
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6
    SETTINGS ordered_group_by_limit_pushdown = 0
) ORDER BY a, b ASC;

-- =====================
-- Composite key with high cardinality (two-level hash table).
-- GROUP BY a, b ORDER BY a, b LIMIT N on high-cardinality data.
-- =====================
SELECT 'composite_two_level';
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(2000000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(2000000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Prefix ORDER BY with high cardinality (two-level hash table).
-- GROUP BY x, y ORDER BY x LIMIT N.
-- Each x has 2 y sub-groups, so LIMIT 10 = 5 full x values.
-- =====================
SELECT 'prefix_two_level';
SELECT * FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(2000000) GROUP BY x, y ORDER BY x ASC LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
) ORDER BY x, y ASC
EXCEPT
SELECT * FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(2000000) GROUP BY x, y ORDER BY x ASC LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 0
) ORDER BY x, y ASC;

DROP TABLE t_gbylimit_comp;
