-- Test: Disjunctions pushdown into JOIN branches
-- This test exercises the optimizer controlled by the setting `use_join_disjunctions_push_down`.
-- It checks that disjunctions (OR) over conjunctions can be split and pushed as per-side
-- pre-join filters without changing query results, and that when the optimization is disabled
-- such pre-join filters are not produced. It also validates join-order-dependent pushdown
SET enable_analyzer=1;

DROP TABLE IF EXISTS tp1;
DROP TABLE IF EXISTS tp2;

CREATE TABLE tp1 (k Int32, a Int32) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE tp2 (k Int32, x Int32) ENGINE = MergeTree() ORDER BY k;

INSERT INTO tp1 VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60);
INSERT INTO tp2 VALUES (1,100),(2,100),(3,200),(4,200),(5,300),(6,300);

-- We need to make sure that query plan creates the JOIN filter only with the optimization enabled, and WHERE filter in both cases

---------- CASE A ----------

SELECT '--- CASE A: plan (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions=1
        SELECT t1.k, t1.a, t2.x
        FROM tp1 AS t1
        JOIN tp2 AS t2 ON t1.k = t2.k
        WHERE (t1.k IN (1,2) AND t2.x = 100) OR (t1.k IN (3,4) AND t2.x = 200)
        ORDER BY t1.k
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

SELECT '--- CASE A: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions=1
        SELECT t1.k, t1.a, t2.x
        FROM tp1 AS t1
        JOIN tp2 AS t2 ON t1.k = t2.k
        WHERE (t1.k IN (1,2) AND t2.x = 100) OR (t1.k IN (3,4) AND t2.x = 200)
        ORDER BY t1.k
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

-- Results identical in both modes (k in {1,2,3,4})
SELECT '--- CASE A: result (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT t1.k, t1.a, t2.x
FROM tp1 AS t1
JOIN tp2 AS t2 ON t1.k = t2.k
WHERE (t1.k IN (1,2) AND t2.x = 100) OR (t1.k IN (3,4) AND t2.x = 200)
ORDER BY t1.k;

SELECT '--- CASE A: result (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT t1.k, t1.a, t2.x
FROM tp1 AS t1
JOIN tp2 AS t2 ON t1.k = t2.k
WHERE (t1.k IN (1,2) AND t2.x = 100) OR (t1.k IN (3,4) AND t2.x = 200)
ORDER BY t1.k;

---------- CASE B ----------

SELECT '--- CASE B: plan (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions=1
        SELECT t1.k, t1.a, t2.x
        FROM tp1 AS t1
        JOIN tp2 AS t2 ON t1.k = t2.k
        WHERE (t2.x = 100) OR (t2.x = 200)
        ORDER BY t1.k
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

SELECT '--- CASE B: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions=1
        SELECT t1.k, t1.a, t2.x
        FROM tp1 AS t1
        JOIN tp2 AS t2 ON t1.k = t2.k
        WHERE (t2.x = 100) OR (t2.x = 200)
        ORDER BY t1.k
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

-- Results identical in both modes (k in {1,2,3,4})
SELECT '--- CASE B: result (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT t1.k, t1.a, t2.x
FROM tp1 AS t1
JOIN tp2 AS t2 ON t1.k = t2.k
WHERE (t2.x = 100) OR (t2.x = 200)
ORDER BY t1.k;

SELECT '--- CASE B: result (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT t1.k, t1.a, t2.x
FROM tp1 AS t1
JOIN tp2 AS t2 ON t1.k = t2.k
WHERE (t2.x = 100) OR (t2.x = 200)
ORDER BY t1.k;

---------- CASE C ----------

SELECT '--- CASE C: plan (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions=1
        SELECT t1.k, t1.a, t2.x
        FROM tp1 AS t1
        JOIN tp2 AS t2 ON t1.k = t2.k
        WHERE (t1.k IN (1,2)) OR (t1.k IN (3,4))
        ORDER BY t1.k
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

SELECT '--- CASE C: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions=1
        SELECT t1.k, t1.a, t2.x
        FROM tp1 AS t1
        JOIN tp2 AS t2 ON t1.k = t2.k
        WHERE (t1.k IN (1,2)) OR (t1.k IN (3,4))
        ORDER BY t1.k
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

-- Results identical in both modes (k in {1,2,3,4})
SELECT '--- CASE C: result (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT t1.k, t1.a, t2.x
FROM tp1 AS t1
JOIN tp2 AS t2 ON t1.k = t2.k
WHERE (t1.k IN (1,2)) OR (t1.k IN (3,4))
ORDER BY t1.k;

SELECT '--- CASE C: result (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT t1.k, t1.a, t2.x
FROM tp1 AS t1
JOIN tp2 AS t2 ON t1.k = t2.k
WHERE (t1.k IN (1,2)) OR (t1.k IN (3,4))
ORDER BY t1.k;

DROP TABLE tp1;
DROP TABLE tp2;

---------- CASE D ----------

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1 (a UInt32, b String) ENGINE = Memory;
CREATE TABLE table2 (c UInt32, d String) ENGINE = Memory;

INSERT INTO table1 VALUES (5, 'a5'), (6, 'a6'), (7, 'a7'), (10, 'a10');
INSERT INTO table2 VALUES (5, 'b5'), (6, 'b6'), (7, 'b7'), (10, 'b10');

SELECT '--- Case D: plan (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions = 1
        SELECT *
        FROM table1
        INNER JOIN table2 ON b = d
        WHERE (a > 0) AND (c > 0)
          AND ( ((a > 5) AND (c < 10)) OR ((a > 6) AND (c < 11)) )
        ORDER BY a ASC, c ASC
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

SELECT '--- Case D: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
        EXPLAIN actions = 1
        SELECT *
        FROM table1
        INNER JOIN table2 ON b = d
        WHERE (a > 0) AND (c > 0)
          AND ( ((a > 5) AND (c < 10)) OR ((a > 6) AND (c < 11)) )
        ORDER BY a ASC, c ASC
    )

WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

SELECT '--- Case D: result (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT *
FROM table1
INNER JOIN table2
    ON (a = c)
WHERE (a > 0) AND (c > 0)
  AND ( ((a > 5) AND (c < 10)) OR ((a > 6) AND (c < 11)) )
ORDER BY a, c
FORMAT TSV;

SELECT '--- Case D: result (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT *
FROM table1
INNER JOIN table2
    ON (a = c)
WHERE (a > 0) AND (c > 0)
  AND ( ((a > 5) AND (c < 10)) OR ((a > 6) AND (c < 11)) )
ORDER BY a, c
FORMAT TSV;

DROP TABLE table1;
DROP TABLE table2;

-- CASE E: every OR-branch contributes at least one pushable atom for that side
SELECT '--- Case E: result (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT n1.number, n2.number
FROM numbers(6) AS n1, numbers(6) AS n2
WHERE ((n1.number = 1 AND n2.number = 2) OR (n1.number = 3 AND n2.number = 4) OR (n1.number = 5))
ORDER BY n1.number, n2.number
FORMAT TSV;

SELECT '--- Case E: result (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT n1.number, n2.number
FROM numbers(6) AS n1, numbers(6) AS n2
WHERE ((n1.number = 1 AND n2.number = 2) OR (n1.number = 3 AND n2.number = 4) OR (n1.number = 5))
ORDER BY n1.number, n2.number
FORMAT TSV;

SELECT '--- Case F: plan (enabled) ---';
SET use_join_disjunctions_push_down = 1;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
    SELECT explain
    FROM (
        EXPLAIN actions = 1
        SELECT
            n1.number,
            n2.number,
            n3.number
        FROM numbers(3) AS n2, numbers(3) AS n3, numbers(3) AS n1
        WHERE ((n1.number = 1) AND ((n2.number + n3.number) = 3))
           OR ((n1.number = 2) AND ((n2.number + n3.number) = 2))
    )
)
WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;

SELECT '--- Case F: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
SELECT REGEXP_REPLACE(trimLeft(explain), '__set_Int32_\\d+_\\d+', '__set_Int32_UNIQ_ID')
FROM (
    SELECT explain
    FROM (
        EXPLAIN actions = 1
        SELECT
            n1.number,
            n2.number,
            n3.number
        FROM numbers(3) AS n2, numbers(3) AS n3, numbers(3) AS n1
        WHERE ((n1.number = 1) AND ((n2.number + n3.number) = 3))
           OR ((n1.number = 2) AND ((n2.number + n3.number) = 2))
    )
)
WHERE explain ILIKE '%Filter column: %' SETTINGS enable_parallel_replicas = 0
FORMAT TSV;
