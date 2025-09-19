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
WITH lines AS (
    SELECT explain
    FROM (EXPLAIN actions=1
          SELECT t1.k, t1.a, t2.x
          FROM tp1 AS t1
          JOIN tp2 AS t2 ON t1.k = t2.k
          WHERE (t1.k IN (1,2) AND t2.x = 100) OR (t1.k IN (3,4) AND t2.x = 200)
          ORDER BY t1.k)
)
SELECT
    -- Count Filters created and pushed into pre-join actions (generic 'Filter' nodes, not 'Prewhere' and not 'Filter (WHERE)')
    countIf(positionCaseInsensitive(explain, 'Filter') > 0
            AND positionCaseInsensitive(explain, '(WHERE') = 0
            AND positionCaseInsensitive(explain, 'Prewhere') = 0
            AND positionCaseInsensitive(explain, 'Filter column:') = 0) AS pre_join_filter_cnt,
    -- Count top-level WHERE filter nodes if they remain after pushdown
    countIf(positionCaseInsensitive(explain, 'Filter (WHERE') > 0) AS where_filter_cnt
FROM lines
FORMAT TSV;

SELECT '--- CASE A: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
WITH lines AS (
    SELECT explain
    FROM (EXPLAIN actions=1
          SELECT t1.k, t1.a, t2.x
          FROM tp1 AS t1
          JOIN tp2 AS t2 ON t1.k = t2.k
          WHERE (t1.k IN (1,2) AND t2.x = 100) OR (t1.k IN (3,4) AND t2.x = 200)
          ORDER BY t1.k)
)
SELECT
    countIf(positionCaseInsensitive(explain, 'Filter') > 0
            AND positionCaseInsensitive(explain, '(WHERE') = 0
            AND positionCaseInsensitive(explain, 'Prewhere') = 0
            AND positionCaseInsensitive(explain, 'Filter column:') = 0) AS pre_join_filter_cnt,
    countIf(positionCaseInsensitive(explain, 'Filter (WHERE') > 0) AS where_filter_cnt
FROM lines
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
WITH lines AS (
    SELECT explain
    FROM (EXPLAIN actions=1
          SELECT t1.k, t1.a, t2.x
          FROM tp1 AS t1
          JOIN tp2 AS t2 ON t1.k = t2.k
          WHERE (t2.x = 100) OR (t2.x = 200)
          ORDER BY t1.k)
)
SELECT
    countIf(positionCaseInsensitive(explain, 'Filter') > 0
            AND positionCaseInsensitive(explain, '(WHERE') = 0
            AND positionCaseInsensitive(explain, 'Prewhere') = 0
            AND positionCaseInsensitive(explain, 'Filter column:') = 0) AS pre_join_filter_cnt,
    countIf(positionCaseInsensitive(explain, 'Filter (WHERE') > 0) AS where_filter_cnt
FROM lines
FORMAT TSV;

SELECT '--- CASE B: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
WITH lines AS (
    SELECT explain
    FROM (EXPLAIN actions=1
          SELECT t1.k, t1.a, t2.x
          FROM tp1 AS t1
          JOIN tp2 AS t2 ON t1.k = t2.k
          WHERE (t2.x = 100) OR (t2.x = 200)
          ORDER BY t1.k)
)
SELECT
    countIf(positionCaseInsensitive(explain, 'Filter') > 0
            AND positionCaseInsensitive(explain, '(WHERE') = 0
            AND positionCaseInsensitive(explain, 'Prewhere') = 0
            AND positionCaseInsensitive(explain, 'Filter column:') = 0) AS pre_join_filter_cnt,
    countIf(positionCaseInsensitive(explain, 'Filter (WHERE') > 0) AS where_filter_cnt
FROM lines
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
WITH lines AS (
    SELECT explain
    FROM (EXPLAIN actions=1
          SELECT t1.k, t1.a, t2.x
          FROM tp1 AS t1
          JOIN tp2 AS t2 ON t1.k = t2.k
          WHERE (t1.k IN (1,2)) OR (t1.k IN (3,4))
          ORDER BY t1.k)
)
SELECT
    countIf(positionCaseInsensitive(explain, 'Filter') > 0
            AND positionCaseInsensitive(explain, '(WHERE') = 0
            AND positionCaseInsensitive(explain, 'Prewhere') = 0
            AND positionCaseInsensitive(explain, 'Filter column:') = 0) AS pre_join_filter_cnt,
    countIf(positionCaseInsensitive(explain, 'Filter (WHERE') > 0) AS where_filter_cnt
FROM lines
FORMAT TSV;

SELECT '--- CASE C: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
WITH lines AS (
    SELECT explain
    FROM (EXPLAIN actions=1
          SELECT t1.k, t1.a, t2.x
          FROM tp1 AS t1
          JOIN tp2 AS t2 ON t1.k = t2.k
          WHERE (t1.k IN (1,2)) OR (t1.k IN (3,4))
          ORDER BY t1.k)
)
SELECT
    countIf(positionCaseInsensitive(explain, 'Filter') > 0
            AND positionCaseInsensitive(explain, '(WHERE') = 0
            AND positionCaseInsensitive(explain, 'Prewhere') = 0
            AND positionCaseInsensitive(explain, 'Filter column:') = 0) AS pre_join_filter_cnt,
    countIf(positionCaseInsensitive(explain, 'Filter (WHERE') > 0) AS where_filter_cnt
FROM lines
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

WITH lines AS (
    SELECT explain
    FROM (
        EXPLAIN actions = 1
        SELECT *
        FROM table1
        INNER JOIN table2 ON b = d
        WHERE (a > 0) AND (c > 0)
          AND ( ((a > 5) AND (c < 10)) OR ((a > 6) AND (c < 11)) )
        ORDER BY a ASC, c ASC
    )
)
SELECT
    countIf(positionCaseInsensitive(explain, '5_UInt')  > 0) AS count_const_5,
    countIf(positionCaseInsensitive(explain, '11_UInt') > 0) AS count_const_11
FROM lines
FORMAT TSV;

SELECT '--- Case D: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;

WITH lines AS (
    SELECT explain
    FROM (
        EXPLAIN actions = 1
        SELECT *
        FROM table1
        INNER JOIN table2 ON b = d
        WHERE (a > 0) AND (c > 0)
          AND ( ((a > 5) AND (c < 10)) OR ((a > 6) AND (c < 11)) )
        ORDER BY a ASC, c ASC
    )
)
SELECT
    countIf(positionCaseInsensitive(explain, '5_UInt')  > 0) AS count_const_5,
    countIf(positionCaseInsensitive(explain, '11_UInt') > 0) AS count_const_11
FROM lines
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
WITH lines AS (
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
SELECT
    -- plus() OR pushed to the (n2 x n3) side
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(plus(') > 0) AS sum_or_push_cnt,
    -- (n1 = 1 OR n1 = 2) pushed to the n1 side
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(__table') > 0
            AND positionCaseInsensitive(explain, ', 1_UInt8') > 0
            AND positionCaseInsensitive(explain, ', 2_UInt8') > 0
            AND positionCaseInsensitive(explain, 'plus(') = 0) AS n1_or_push_cnt
FROM lines
FORMAT TSV;

SELECT '--- Case F: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
WITH lines AS (
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
SELECT
    -- plus() OR pushed to the (n2 x n3) side
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(plus(') > 0) AS sum_or_push_cnt,
    -- (n1 = 1 OR n1 = 2) pushed to the n1 side
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(__table') > 0
            AND positionCaseInsensitive(explain, ', 1_UInt8') > 0
            AND positionCaseInsensitive(explain, ', 2_UInt8') > 0
            AND positionCaseInsensitive(explain, 'plus(') = 0) AS n1_or_push_cnt
FROM lines
FORMAT TSV;

SELECT '--- Case F: plan (enabled) ---';
SET use_join_disjunctions_push_down = 1;
WITH lines AS (
    SELECT explain
    FROM (
        EXPLAIN actions = 1
        SELECT
            n1.number,
            n2.number,
            n3.number
        FROM numbers(3) AS n1, numbers(3) AS n2, numbers(3) AS n3
        WHERE ((n1.number = 1) AND ((n2.number + n3.number) = 3))
           OR ((n1.number = 2) AND ((n2.number + n3.number) = 2))
    )
)
SELECT
    -- plus() OR cannot be pushed until both n2 and n3 are present in this order
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(plus(') > 0) AS sum_or_push_cnt,
    -- (n1 = 1 OR n1 = 2) pushed to the n1 side
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(__table') > 0
            AND positionCaseInsensitive(explain, ', 1_UInt8') > 0
            AND positionCaseInsensitive(explain, ', 2_UInt8') > 0
            AND positionCaseInsensitive(explain, 'plus(') = 0) AS n1_or_push_cnt
FROM lines
FORMAT TSV;

SELECT '--- Case F: plan (disabled) ---';
SET use_join_disjunctions_push_down = 0;
WITH lines AS (
    SELECT explain
    FROM (
        EXPLAIN actions = 1
        SELECT
            n1.number,
            n2.number,
            n3.number
        FROM numbers(3) AS n1, numbers(3) AS n2, numbers(3) AS n3
        WHERE ((n1.number = 1) AND ((n2.number + n3.number) = 3))
           OR ((n1.number = 2) AND ((n2.number + n3.number) = 2))
    )
)
SELECT
    -- plus() OR cannot be pushed until both n2 and n3 are present in this order
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(plus(') > 0) AS sum_or_push_cnt,
    -- (n1 = 1 OR n1 = 2) pushed to the n1 side
    countIf(positionCaseInsensitive(explain, 'Filter column: or(equals(__table') > 0
            AND positionCaseInsensitive(explain, ', 1_UInt8') > 0
            AND positionCaseInsensitive(explain, ', 2_UInt8') > 0
            AND positionCaseInsensitive(explain, 'plus(') = 0) AS n1_or_push_cnt
FROM lines
FORMAT TSV;
