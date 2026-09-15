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

SELECT '-- An enclosing INNER JOIN allows converting under join_use_nulls = 1.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A Nullable join key allows converting under join_use_nulls = 0.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0, join_use_nulls = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS join_use_nulls = 0;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    SETTINGS join_use_nulls = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A non-Nullable key under join_use_nulls = 0 does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    SETTINGS join_use_nulls = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- IS NOT DISTINCT FROM does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val IS NOT DISTINCT FROM s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing FULL join does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id FULL JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An inequality condition allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val < s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val < s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val < s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- The enclosing join need not be the direct parent.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A filter over three relations stays above both joins and allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id WHERE m.val + o.id > f.v
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id WHERE m.val + o.id > f.v;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id WHERE m.val + o.id > f.v
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- Without the transitive pass the same filter does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id WHERE m.val + o.id > f.v
    SETTINGS query_plan_convert_outer_join_to_inner_join_transitively = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A RIGHT join below allows converting.';
SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing RIGHT join allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing ANY join allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing LEFT SEMI join allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- An enclosing RIGHT SEMI join allows converting.';
SELECT count(), sum(s.val) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT SEMI JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(s.val) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT SEMI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(s.val) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT SEMI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- An enclosing RIGHT ANTI join allows converting.';
SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT ANTI JOIN small AS s ON m.val = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

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

SELECT '-- A WHERE over two relations above the join allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN mid_nullable AS n ON f.id = n.id WHERE m.val = n.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN mid_nullable AS n ON f.id = n.id WHERE m.val = n.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN mid_nullable AS n ON f.id = n.id WHERE m.val = n.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- A PASTE JOIN does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(y.n) FROM (SELECT f.id AS lid, m.val AS rval FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id) AS x PASTE JOIN (SELECT number AS n FROM numbers(200)) AS y WHERE x.rval IS NOT NULL
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A FULL join with a rejected left column becomes LEFT.';
SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A FULL join with a rejected right column becomes RIGHT.';
SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.mval = s.val
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.mval = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN small AS s ON g.mval = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Type: PASTE');

SELECT '-- A FULL join rejected on both sides becomes INNER.';
SELECT count() FROM (SELECT f.id AS fid, m.val AS mval FROM fact AS f FULL JOIN mid_nullable AS m ON f.id = m.id) AS g INNER JOIN other AS o ON g.fid = o.id AND g.mval = o.id
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

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
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, query_plan_convert_outer_join_to_inner_join = 0;

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
