-- Tags: long
-- Test which plan steps a null-rejecting constraint may cross on its way down to the outer join.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_outer_join_to_inner_join_transitively = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS mid;
DROP TABLE IF EXISTS small;

CREATE TABLE fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 20, number FROM numbers(100);
CREATE TABLE mid (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, number % 5 FROM numbers(10);
CREATE TABLE small (val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT 2 * number + 1 FROM numbers(2);

SELECT '-- DISTINCT allows converting.';
SELECT count() FROM (SELECT DISTINCT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT DISTINCT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A window partitioned by the join key allows converting.';
SELECT count(), sum(g.c) FROM (SELECT m.val AS val, count() OVER (PARTITION BY m.val) AS c FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(g.c) FROM (SELECT m.val AS val, count() OVER (PARTITION BY m.val) AS c FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A window partitioned by another column does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(g.c) FROM (SELECT m.val AS val, count() OVER (PARTITION BY f.id) AS c FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- LIMIT BY the join key allows converting.';
SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id ORDER BY val LIMIT 1 BY m.val) AS g INNER JOIN small AS s ON g.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id ORDER BY val LIMIT 1 BY m.val) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- LIMIT BY with an OFFSET does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id ORDER BY val LIMIT 1 OFFSET 1 BY m.val) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- LIMIT does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id ORDER BY val NULLS FIRST LIMIT 20) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A GROUP BY key allows converting.';
SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id GROUP BY m.val) AS g INNER JOIN small AS s ON g.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id GROUP BY m.val) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An aggregate result does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.id AS id, max(m.val) AS mv FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id GROUP BY m.id) AS g INNER JOIN small AS s ON g.mv = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- GROUPING SETS does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id GROUP BY GROUPING SETS ((m.val), (m.id))) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- ARRAY JOIN of another column allows converting.';
SELECT count(), sum(g.z) FROM (SELECT m.val AS val, z FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id ARRAY JOIN [1, 2] AS z) AS g INNER JOIN small AS s ON g.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(g.z) FROM (SELECT m.val AS val, z FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id ARRAY JOIN [1, 2] AS z) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- ARRAY JOIN of the join key does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT arr AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id ARRAY JOIN [m.val] AS arr) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- UNION ALL with matching branches allows converting.';
SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id UNION ALL SELECT toNullable(val) FROM small) AS g INNER JOIN small AS s ON g.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id UNION ALL SELECT toNullable(val) FROM small) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An IN subquery allows converting.';
SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id WHERE f.id IN (SELECT val FROM small)) AS g INNER JOIN small AS s ON g.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.val AS val FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id WHERE f.id IN (SELECT val FROM small)) AS g INNER JOIN small AS s ON g.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

DROP TABLE fact;
DROP TABLE mid;
DROP TABLE small;
