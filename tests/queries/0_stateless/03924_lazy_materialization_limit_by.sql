-- Test that lazy materialization works with LIMIT BY and DISTINCT ON queries
-- Tags: no-random-settings

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_limit_by_lm;

CREATE TABLE t_limit_by_lm (a UInt64, b UInt64, c String, d String)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_limit_by_lm SELECT number, number % 10, toString(number), toString(number * 2)
FROM numbers(10000);

-- Test 1: EXPLAIN shows lazy materialization for LIMIT BY query
SELECT '=== Test 1: LIMIT BY EXPLAIN ===';

SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') AS lazy_read_count,
       countIf(explain LIKE '%JoinLazyColumnsStep%') AS join_lazy_count
FROM (
    EXPLAIN
    SELECT * FROM t_limit_by_lm ORDER BY a LIMIT 2 BY b LIMIT 5
);

-- Test 2: EXPLAIN shows lazy materialization for DISTINCT ON query
SELECT '=== Test 2: DISTINCT ON EXPLAIN ===';

SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') AS lazy_read_count,
       countIf(explain LIKE '%JoinLazyColumnsStep%') AS join_lazy_count
FROM (
    EXPLAIN
    SELECT DISTINCT ON (b) * FROM t_limit_by_lm ORDER BY a LIMIT 5
);

-- Test 3: Correctness for LIMIT BY
SELECT '=== Test 3: LIMIT BY correctness ===';

SELECT * FROM t_limit_by_lm ORDER BY a LIMIT 2 BY b LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1;

SELECT * FROM t_limit_by_lm ORDER BY a LIMIT 2 BY b LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;

-- Test 4: Correctness for DISTINCT ON
SELECT '=== Test 4: DISTINCT ON correctness ===';

SELECT DISTINCT ON (b) * FROM t_limit_by_lm ORDER BY a LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1;

SELECT DISTINCT ON (b) * FROM t_limit_by_lm ORDER BY a LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;

-- Test 5: Correctness for LIMIT BY with WHERE filter
SELECT '=== Test 5: LIMIT BY with WHERE ===';

SELECT * FROM t_limit_by_lm WHERE a < 100 ORDER BY a LIMIT 1 BY b LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1;

SELECT * FROM t_limit_by_lm WHERE a < 100 ORDER BY a LIMIT 1 BY b LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;

-- Test 6: EXPLAIN for DISTINCT ON with multiple columns
SELECT '=== Test 6: DISTINCT ON multi-column EXPLAIN ===';

SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') AS lazy_read_count,
       countIf(explain LIKE '%JoinLazyColumnsStep%') AS join_lazy_count
FROM (
    EXPLAIN
    SELECT DISTINCT ON (a, b) * FROM t_limit_by_lm ORDER BY a LIMIT 5
);

-- Test 7: Correctness for DISTINCT ON with multiple columns
SELECT '=== Test 7: DISTINCT ON multi-column correctness ===';

SELECT DISTINCT ON (a, b) * FROM t_limit_by_lm ORDER BY a LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1;

SELECT DISTINCT ON (a, b) * FROM t_limit_by_lm ORDER BY a LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS t_limit_by_lm;
