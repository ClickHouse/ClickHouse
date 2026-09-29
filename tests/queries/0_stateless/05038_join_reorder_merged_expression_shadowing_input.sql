-- https://github.com/ClickHouse/ClickHouse/issues/116036

DROP TABLE IF EXISTS t_a_05038;
DROP TABLE IF EXISTS t_b_05038;

CREATE TABLE t_a_05038 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_b_05038 (c0 Int32) ENGINE = MergeTree ORDER BY c0;

INSERT INTO t_a_05038 VALUES (1), (2), (3);
INSERT INTO t_b_05038 VALUES (10), (20);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 16;
SET query_plan_optimize_join_order_algorithm = 'greedy';
-- The cost model is randomized in CI and picks a different join order, so pin it.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_expression_into_join = 1;

SELECT '-- shadowing output, values must be equal on both sides of ON';
SELECT a.c0, v.c0
FROM t_a_05038 AS a
INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
ON a.c0 = v.c0
ORDER BY ALL;

SELECT '-- row count';
SELECT count()
FROM t_a_05038 AS a
INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
ON a.c0 = v.c0;

SELECT '-- shadowing output that also changes the type must plan and return equal values';
SELECT a.c0, v.c0
FROM t_a_05038 AS a
INNER JOIN view(SELECT x.c0 + 1 AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
ON a.c0 = v.c0
ORDER BY ALL;

SELECT '-- three inner relations, two flatten levels';
SELECT a.c0, v.c0
FROM t_a_05038 AS a
INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y, t_b_05038 AS z) AS v
ON a.c0 = v.c0
ORDER BY ALL;

SELECT '-- non-shadowing output keeps its values';
SELECT a.c0, v.d0
FROM t_a_05038 AS a
INNER JOIN view(SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
ON a.c0 = v.d0
ORDER BY ALL;

SELECT '-- shadowing only on an internal node keeps its values';
SELECT a.c0, v.d0
FROM t_a_05038 AS a
INNER JOIN view(SELECT toInt32(t.c0 + 1) AS d0 FROM (SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS t) AS v
ON a.c0 = v.d0
ORDER BY ALL;

-- All three relation labels appear in one join label only when the inner join was flattened into
-- the parent graph, so a `1` here means the expression is still merged. Label row estimates vary
-- with randomized statistics settings, so match the relation names and not the estimates.
-- The settings below are repeated per EXPLAIN because a `SETTINGS` clause on the explained query
-- is what the runner's randomized values cannot override.
SELECT '-- non-shadowing output is still merged into the join graph';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.d0
    FROM t_a_05038 AS a
    INNER JOIN view(SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
    ON a.c0 = v.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- shadowing only on an internal node is still merged into the join graph';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.d0
    FROM t_a_05038 AS a
    INNER JOIN view(SELECT toInt32(t.c0 + 1) AS d0 FROM (SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS t) AS v
    ON a.c0 = v.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- shadowing output is not merged into the join graph';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.c0
    FROM t_a_05038 AS a
    INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
    ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- nothing is merged when the setting is off';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.d0
    FROM t_a_05038 AS a
    INNER JOIN view(SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
    ON a.c0 = v.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 0
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

-- Refusing the merge must not disable the reordering of the parent join. Only a relation the join
-- order optimizer costed carries a bracketed label, so this reads `0` when reordering never ran,
-- which the arms above cannot tell apart from a refused merge.
SELECT '-- the parent join of a refused merge is still reordered';
SELECT count() > 0 FROM (
    EXPLAIN SELECT a.c0, v.c0
    FROM t_a_05038 AS a
    INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
    ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%';

SELECT '-- and the same query is not reordered once reordering is off';
SELECT count() > 0 FROM (
    EXPLAIN SELECT a.c0, v.c0
    FROM t_a_05038 AS a
    INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y) AS v
    ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%';

-- A stored view pushes a second expression step converting the subquery result to the view
-- structure, so it reaches the peel through a different plan shape than `view(...)` does.
DROP VIEW IF EXISTS v_shadow_05038;
DROP VIEW IF EXISTS v_plain_05038;
CREATE VIEW v_shadow_05038 AS SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05038 AS x, t_b_05038 AS y;
CREATE VIEW v_plain_05038 AS SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05038 AS x, t_b_05038 AS y;

SELECT '-- stored view with a shadowing output, values must be equal on both sides of ON';
SELECT a.c0, v.c0
FROM t_a_05038 AS a
INNER JOIN v_shadow_05038 AS v
ON a.c0 = v.c0
ORDER BY ALL;

SELECT '-- stored view row count';
SELECT count()
FROM t_a_05038 AS a
INNER JOIN v_shadow_05038 AS v
ON a.c0 = v.c0;

SELECT '-- stored view shadowing output is not merged into the join graph';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.c0 FROM t_a_05038 AS a INNER JOIN v_shadow_05038 AS v ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- stored view non-shadowing output is still merged into the join graph';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, w.d0 FROM t_a_05038 AS a INNER JOIN v_plain_05038 AS w ON a.c0 = w.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

-- The stored view reaches the peel through its own plan shape, so it needs its own reordering pair.
SELECT '-- the parent join of a refused stored view merge is still reordered';
SELECT count() > 0 FROM (
    EXPLAIN SELECT a.c0, v.c0 FROM t_a_05038 AS a INNER JOIN v_shadow_05038 AS v ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%';

SELECT '-- and the same stored view query is not reordered once reordering is off';
SELECT count() > 0 FROM (
    EXPLAIN SELECT a.c0, v.c0 FROM t_a_05038 AS a INNER JOIN v_shadow_05038 AS v ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%';

DROP VIEW v_plain_05038;
DROP VIEW v_shadow_05038;
DROP TABLE t_b_05038;
DROP TABLE t_a_05038;
