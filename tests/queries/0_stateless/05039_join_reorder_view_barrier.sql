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

-- A view whose own join is itself a candidate for reordering is still kept whole. This arm does not
-- cover the boundary being carried onto the node `chooseJoinOrder` returns: the traversal optimizes
-- the enclosing join first, so it reads the boundary before the inner join is ever replaced. Measured
-- by deleting that re-application, which leaves every arm of this test green. It is there for a
-- receiver handed an already-expanded fragment, which the arms below cover.
SELECT '-- view whose own inner join is reordered';
SELECT a.c0, v.c0
FROM t_a_05039 AS a
INNER JOIN view(
    SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y, t_c_05039 AS z WHERE z.c0 = x.c0
) AS v
ON a.c0 = v.c0
ORDER BY ALL;

-- Being opaque to the enclosing graph must not stop a view from reordering its OWN join. Only a costed
-- relation carries a bracketed label, and the enclosing graph costs the view as a whole, so a label
-- naming the view's own relations and not the outer alias can only come from the view's own reordering.
-- All THREE of its relations are required in ONE label, which is the view's topmost join: requiring
-- only two also matches that join's own child, which survives even when the topmost one is left
-- uncosted. Paired with a reordering-off control, so the arm cannot pass on the label text alone.
SELECT '-- a view reorders its own join';
SELECT count() > 0 FROM (
    EXPLAIN SELECT a.c0, v.c0
    FROM t_a_05039 AS a
    INNER JOIN view(
        SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y, t_c_05039 AS z WHERE z.c0 = x.c0
    ) AS v
    ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0
) WHERE explain LIKE '%x[%' AND explain LIKE '%y[%' AND explain LIKE '%z[%' AND explain NOT LIKE '%a[%';

SELECT '-- and costs nothing once reordering is off';
SELECT count() > 0 FROM (
    EXPLAIN SELECT a.c0, v.c0
    FROM t_a_05039 AS a
    INNER JOIN view(
        SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y, t_c_05039 AS z WHERE z.c0 = x.c0
    ) AS v
    ON a.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0
) WHERE explain LIKE '%x[%' AND explain LIKE '%y[%' AND explain LIKE '%z[%' AND explain NOT LIKE '%a[%';

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

-- An inlined view is rewritten into a subquery before the join tree is resolved, so it is numbered by
-- the same alias pass as the enclosing query, does not restart the namespace, and is not a case the
-- barrier exists for. Only a stored view reaches the inlining hook, which rewrites a `TableNode`, so a
-- `view(...)` table function never enters it. The pair reads different values, which is what shows the
-- setting is in effect here instead of being asserted by results that agree either way. Suppressing the
-- marking under the setting leaves both arms unchanged, so the inlined arm flattens because no view is
-- read at all, not because the mark was withheld.
SELECT '-- a stored view is flattened once it is inlined';
DROP VIEW IF EXISTS v_inline_05039;
CREATE VIEW v_inline_05039 AS SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05039 AS x, t_b_05039 AS y;
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.d0
    FROM t_a_05039 AS a
    INNER JOIN v_inline_05039 AS v
    ON a.c0 = v.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0, analyzer_inline_views = 1
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- and is opaque when it is not';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, v.d0
    FROM t_a_05039 AS a
    INNER JOIN v_inline_05039 AS v
    ON a.c0 = v.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0, analyzer_inline_views = 0
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

-- Values must be correct on the inlined path too, so read a stored view whose output name shadows its
-- own input's.
SELECT '-- an inlined view whose output shadows its own input name';
DROP VIEW IF EXISTS v_shadow_05039;
CREATE VIEW v_shadow_05039 AS SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y;
SELECT a.c0, v.c0
FROM t_a_05039 AS a
INNER JOIN v_shadow_05039 AS v
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
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- a plain subquery still flattens into it';
SELECT count() FROM (
    EXPLAIN SELECT a.c0, s.d0
    FROM t_a_05039 AS a
    INNER JOIN (SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05039 AS x, t_b_05039 AS y) AS s
    ON a.c0 = s.d0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

-- Making the view opaque must not stop the enclosing join from being reordered: the relations beside the
-- view are still costed, so `b` and `c` carry a bracketed cost annotation while the view appears as its
-- bare alias, being opaque and therefore uncosted. Require both costed relations and that alias in ONE
-- join label, because the traversal continues into children and a label produced by an independently
-- reordered descendant would otherwise satisfy an arm that only asked for one of them. Paired with a
-- reordering-off control, so it cannot pass on a query the optimizer skipped entirely.
SELECT '-- relations beside an opaque view are still reordered';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count()
    FROM t_b_05039 AS b, t_c_05039 AS c,
         view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
    WHERE b.c0 = v.c0 AND c.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0
) WHERE explain LIKE '%b[%' AND explain LIKE '%c[%' AND explain LIKE '% v %';

SELECT '-- and nothing is costed once reordering is off';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count()
    FROM t_b_05039 AS b, t_c_05039 AS c,
         view(SELECT toInt32(x.c0 + 1) AS c0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
    WHERE b.c0 = v.c0 AND c.c0 = v.c0
    SETTINGS query_plan_optimize_join_order_limit = 0, query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0
) WHERE explain LIKE '%b[%' AND explain LIKE '%c[%' AND explain LIKE '% v %';

-- No view is read on a replica handed an already-expanded plan fragment, so the boundary cannot be
-- re-derived there and has to travel with the step itself. Decorrelating a correlated subquery without
-- the in-memory buffer emits a subplan reference, which is materialized by cloning the referenced
-- subplan, and that clone happens before join reordering runs, so a boundary lost in `clone` leaves a
-- flattenable view behind. Both copies of the view join must stay opaque; the plain-subquery arm after
-- it is the control that this shape does flatten what it may.
SELECT '-- a cloned subplan keeps the view opaque';
SELECT count() FROM (
    EXPLAIN WITH w AS (
        SELECT a.c0 AS o0, v.d0 AS o1
        FROM t_a_05039 AS a
        INNER JOIN view(SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05039 AS x, t_b_05039 AS y) AS v
        ON a.c0 = v.d0
    )
    SELECT *, (SELECT w.o0 WHERE or(173, w.o0 >= 0)) AS r FROM w ORDER BY 1
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0, allow_experimental_correlated_subqueries = 1,
             correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'right'
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

SELECT '-- and still flattens a plain subquery in both copies';
SELECT count() FROM (
    EXPLAIN WITH w AS (
        SELECT a.c0 AS o0, s.d0 AS o1
        FROM t_a_05039 AS a
        INNER JOIN (SELECT toInt32(x.c0 + 1) AS d0 FROM t_a_05039 AS x, t_b_05039 AS y) AS s
        ON a.c0 = s.d0
    )
    SELECT *, (SELECT w.o0 WHERE or(173, w.o0 >= 0)) AS r FROM w ORDER BY 1
    SETTINGS query_plan_optimize_join_order_limit = 16, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_merge_expression_into_join = 1,
             enable_parallel_replicas = 0, allow_experimental_correlated_subqueries = 1,
             correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_default_join_kind = 'right'
) WHERE explain LIKE '%a[%' AND explain LIKE '%x[%' AND explain LIKE '%y[%';

-- The wire is the other way a step travels, and it is deliberately NOT asserted here. Setting
-- `serialize_query_plan = 1` is not enough to reach `serialize`/`deserialize`: a plan is only written
-- to a stream for a shard that is actually remote, and a single-server test has none, so the local
-- shard takes the local-plan path instead. Measured by instrumenting both functions and running this
-- shape, `remote()` over this server's own port, a `remote()` that ships the whole view join, and two
-- joined remote legs: zero calls in every one, while a probe at `clone` in the same build fired. An
-- arm here would pass whatever the `flags` byte did, so none is shipped; the round trip is asserted
-- in `gtest_join_step_logical_reorder_boundary_roundtrip.cpp` instead.
DROP VIEW v_shadow_05039;
DROP VIEW v_inline_05039;
DROP VIEW v_merge_05039;
DROP TABLE t_c_05039;
DROP TABLE t_b_05039;
DROP TABLE t_a_05039;
