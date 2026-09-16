-- Tags: long
-- Test outer to inner join conversion driven by a null-rejecting constraint that is proven
-- arbitrarily higher in the plan: by a filter, or by the predicate of an enclosing join.

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
DROP TABLE IF EXISTS mid_nullable;
DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS other;
DROP TABLE IF EXISTS storage_join;

CREATE TABLE fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 20, number FROM numbers(100);
CREATE TABLE mid (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, number % 5 FROM numbers(10);
CREATE TABLE mid_nullable (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 2 = 0, NULL, number % 5) FROM numbers(10);
CREATE TABLE small (val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT 2 * number + 1 FROM numbers(2);
CREATE TABLE other (id UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number FROM numbers(20);
CREATE TABLE storage_join (val UInt64, s Nullable(String)) ENGINE = Join(ALL, LEFT, val);

INSERT INTO storage_join VALUES (1, 'a'), (3, 'b');

SELECT '-- A stateful function in the filter does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT m.id AS k, s.val AS y FROM mid_nullable AS m LEFT JOIN small AS s ON m.val = s.val ORDER BY k) WHERE y IS NOT NULL AND rowNumberInAllBlocks() < 2
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A non-deterministic function between the filter and the join does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT g.r FROM (SELECT m.val AS k, rand64() AS r FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.k = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A non-deterministic function in the enclosing join condition does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + rand64() % 2 = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A RIGHT join below allows converting.';
SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing RIGHT join allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing ANY join allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing LEFT SEMI join allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- An enclosing RIGHT ANTI join allows converting.';
SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT ANTI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT ANTI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- An enclosing LEFT ANTI join does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT ANTI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- A PASTE JOIN does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(y.n) FROM (SELECT f.id AS lid, m.val AS rval FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id) AS x PASTE JOIN (SELECT number AS n FROM numbers(200)) AS y WHERE x.rval IS NOT NULL
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A FULL join with a rejected left column becomes LEFT.';
SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A FULL join with a rejected right column becomes RIGHT.';
SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.mval = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.mval = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A FULL join rejected on both sides becomes INNER.';
SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id AND g.mval = o.id;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id AND g.mval = o.id
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A `Join` engine source does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT f.id AS id, j.s AS s FROM fact AS f LEFT JOIN storage_join AS j ON f.id = j.val) AS g INNER JOIN (SELECT 'a' AS str) AS t ON g.s = t.str SETTINGS join_use_nulls = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A distributed plan fragment allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

DROP TABLE fact;
DROP TABLE mid;
DROP TABLE mid_nullable;
DROP TABLE small;
DROP TABLE other;
DROP TABLE storage_join;
