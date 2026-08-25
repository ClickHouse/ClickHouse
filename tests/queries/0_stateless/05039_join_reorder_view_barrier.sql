-- A view is analyzed on its own, so its relations are numbered from `__table1` again and can carry the
-- same qualified names as the enclosing query's. The join order optimizer therefore treats a view's own
-- topmost join as one opaque relation instead of flattening its relations into the enclosing join graph.
-- Values must be unaffected; only the plan shape changes.

DROP TABLE IF EXISTS t_a_05039;
DROP TABLE IF EXISTS t_b_05039;
DROP TABLE IF EXISTS t_c_05039;

CREATE TABLE t_a_05039 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_b_05039 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_c_05039 (c0 Int32) ENGINE = MergeTree ORDER BY c0;

INSERT INTO t_a_05039 VALUES (1), (2), (3);
INSERT INTO t_b_05039 VALUES (10), (20);
INSERT INTO t_c_05039 VALUES (2), (3);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 16;
SET query_plan_optimize_join_order_algorithm = 'greedy';
-- The cost model is randomized in CI and would pick a different join order, so pin it.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_expression_into_join = 1;

SELECT '-- view whose output shadows its own input name';
SELECT a.c0, v.c0
FROM t_a_05039 AS a
INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
ON a.c0 = v.c0
ORDER BY ALL;

SELECT '-- view whose output does not shadow an input name';
SELECT a.c0, v.d0
FROM t_a_05039 AS a
INNER JOIN view(SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
ON a.c0 = v.d0
ORDER BY ALL;

-- The view's own join is reordered internally before the enclosing join is optimized, and
-- `chooseJoinOrder` replaces that node, so this arm covers the boundary surviving the replacement.
SELECT '-- view whose own inner join is reordered';
SELECT a.c0, v.c0
FROM t_a_05039 AS a
INNER JOIN view(
    SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y, t_c_05039 AS z WHERE z.c0 = x.c0
) AS v
ON a.c0 = v.c0
ORDER BY ALL;

SELECT '-- merge() inside a view';
DROP VIEW IF EXISTS v_merge_05039;
CREATE VIEW v_merge_05039 AS
    SELECT toInt32(x.c0 + 1) AS c0
    FROM merge(currentDatabase(), '^t_a_05039$') AS x, t_b_05039 AS y;
SELECT a.c0, m.c0
FROM t_a_05039 AS a
INNER JOIN v_merge_05039 AS m
ON a.c0 = m.c0
ORDER BY ALL;

-- An inlined view is numbered by the same alias pass as the enclosing query, so it is not a case the
-- barrier exists for; it is pinned here because it reaches a different code path into the view.
SELECT '-- view read with analyzer_inline_views';
SELECT a.c0, v.c0
FROM t_a_05039 AS a
INNER JOIN view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
ON a.c0 = v.c0
ORDER BY ALL
SETTINGS analyzer_inline_views = 1;

-- A relation that does NOT restart the identifier namespace keeps its flattening, so these pin that the
-- barrier is not over-broad. Both were already correct before it.
SELECT '-- plain subquery values';
SELECT a.c0, s.c0
FROM t_a_05039 AS a
INNER JOIN (SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y) AS s
ON a.c0 = s.c0
ORDER BY ALL;

SELECT '-- CTE values';
WITH w AS (SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y)
SELECT a.c0, w.c0
FROM t_a_05039 AS a
INNER JOIN w
ON a.c0 = w.c0
ORDER BY ALL;

-- Only a relation the join order optimizer costed carries a bracketed label, so all three labels appear
-- in one join label only when the inner relations were flattened into the enclosing graph. A view is
-- opaque, so it reads 0; a plain subquery is not, so it reads 1. The `SETTINGS` clause is repeated on
-- each EXPLAIN because that is what the runner's randomized values cannot override.
SELECT '-- a view is one relation of the enclosing join graph';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.d0
    FROM t_a_05039 AS a
    INNER JOIN view(SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
    ON a.c0 = v.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- a plain subquery still flattens into it';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, s.d0
    FROM t_a_05039 AS a
    INNER JOIN (SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05039 AS x, t_b_05039 AS y) AS s
    ON a.c0 = s.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

-- Making the view opaque must not stop the enclosing join from being reordered: the relations beside the
-- view are still costed. Paired with a reordering-off control, so it cannot pass on a query the
-- optimizer skipped entirely.
SELECT '-- relations beside an opaque view are still reordered';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count()
    FROM t_b_05039 AS b, t_c_05039 AS c,
         view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
    WHERE b.c0 = v.c0 AND c.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%b[%' OR explain LIKE '%c[%';

SELECT '-- and nothing is costed once reordering is off';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count()
    FROM t_b_05039 AS b, t_c_05039 AS c,
         view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
    WHERE b.c0 = v.c0 AND c.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_merge_expression_into_join = 1
) WHERE explain LIKE '%b[%' OR explain LIKE '%c[%';

DROP VIEW v_merge_05039;
DROP TABLE t_c_05039;
DROP TABLE t_b_05039;
DROP TABLE t_a_05039;
