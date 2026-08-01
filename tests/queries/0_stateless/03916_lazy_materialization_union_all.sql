-- Test that lazy materialization is applied to all branches of a UNION ALL query
-- Tags: no-random-settings

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_union_lm_1;
DROP TABLE IF EXISTS t_union_lm_2;
DROP TABLE IF EXISTS t_union_lm_3;

CREATE TABLE t_union_lm_1 (a UInt64, b UInt64, c String, d String)
ENGINE = MergeTree ORDER BY (a, b);

CREATE TABLE t_union_lm_2 (a UInt64, b UInt64, c String, d String)
ENGINE = MergeTree ORDER BY (a, b);

CREATE TABLE t_union_lm_3 (a UInt64, b UInt64, c String, d String)
ENGINE = MergeTree ORDER BY (a, b);

INSERT INTO t_union_lm_1 SELECT number % 10, number, toString(number), toString(number * 2)
FROM numbers(10000);

INSERT INTO t_union_lm_2 SELECT number % 10, number, toString(number), toString(number * 2)
FROM numbers(10000);

INSERT INTO t_union_lm_3 SELECT number % 10, number, toString(number), toString(number * 2)
FROM numbers(10000);

-- Test 1: Both branches of a two-table UNION ALL should get lazy materialization
SELECT '=== Test 1: Two branches UNION ALL ===';

SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') AS lazy_read_count,
       countIf(explain LIKE '%JoinLazyColumnsStep%') AS join_lazy_count
FROM (
    EXPLAIN
    SELECT * FROM (
        SELECT * FROM t_union_lm_1 WHERE a = 1 ORDER BY b DESC LIMIT 10
        UNION ALL
        SELECT * FROM t_union_lm_2 WHERE a = 2 ORDER BY b DESC LIMIT 10
    ) ORDER BY b DESC LIMIT 5
);

-- Test 2: All three branches should get lazy materialization
SELECT '=== Test 2: Three branches UNION ALL ===';

SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') AS lazy_read_count,
       countIf(explain LIKE '%JoinLazyColumnsStep%') AS join_lazy_count
FROM (
    EXPLAIN
    SELECT * FROM (
        SELECT * FROM t_union_lm_1 WHERE a = 1 ORDER BY b DESC LIMIT 10
        UNION ALL
        SELECT * FROM t_union_lm_2 WHERE a = 2 ORDER BY b DESC LIMIT 10
        UNION ALL
        SELECT * FROM t_union_lm_3 WHERE a = 3 ORDER BY b DESC LIMIT 10
    ) ORDER BY b DESC LIMIT 5
);

-- Test 3: Correctness — verify results match with optimization disabled
SELECT '=== Test 3: Correctness check ===';

SELECT * FROM (
    SELECT * FROM t_union_lm_1 WHERE a = 1 ORDER BY b DESC LIMIT 10
    UNION ALL
    SELECT * FROM t_union_lm_2 WHERE a = 2 ORDER BY b DESC LIMIT 10
) ORDER BY b DESC LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1;

SELECT * FROM (
    SELECT * FROM t_union_lm_1 WHERE a = 1 ORDER BY b DESC LIMIT 10
    UNION ALL
    SELECT * FROM t_union_lm_2 WHERE a = 2 ORDER BY b DESC LIMIT 10
) ORDER BY b DESC LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;

-- Test 4: Correctness with three branches
SELECT '=== Test 4: Three branches correctness ===';

SELECT * FROM (
    SELECT * FROM t_union_lm_1 WHERE a = 1 ORDER BY b DESC LIMIT 5
    UNION ALL
    SELECT * FROM t_union_lm_2 WHERE a = 2 ORDER BY b DESC LIMIT 5
    UNION ALL
    SELECT * FROM t_union_lm_3 WHERE a = 3 ORDER BY b DESC LIMIT 5
) ORDER BY b DESC LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1;

SELECT * FROM (
    SELECT * FROM t_union_lm_1 WHERE a = 1 ORDER BY b DESC LIMIT 5
    UNION ALL
    SELECT * FROM t_union_lm_2 WHERE a = 2 ORDER BY b DESC LIMIT 5
    UNION ALL
    SELECT * FROM t_union_lm_3 WHERE a = 3 ORDER BY b DESC LIMIT 5
) ORDER BY b DESC LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS t_union_lm_1;
DROP TABLE IF EXISTS t_union_lm_2;
DROP TABLE IF EXISTS t_union_lm_3;
