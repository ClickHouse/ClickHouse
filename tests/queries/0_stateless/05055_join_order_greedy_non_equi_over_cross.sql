-- Greedy join reordering must consider pairs linked only by non-equi predicates instead of
-- committing to a bare cross product that happens to be enumerated first.

SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join order
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 'false';
SET join_algorithm = 'hash';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_analyzer = 1;

CREATE TABLE ta (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tb (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tc (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ta SELECT number FROM numbers(100);
INSERT INTO tb SELECT number FROM numbers(100);
INSERT INTO tc SELECT number FROM numbers(10);

-- ta and tb are linked to tc only by range predicates, while ta x tb has no predicate at all,
-- so the plan must join tc to one of them first and not start from the ta x tb cross product.
EXPLAIN
SELECT count()
FROM ta CROSS JOIN tb INNER JOIN tc ON ta.k < tc.k AND tb.k < tc.k;

SELECT count()
FROM ta CROSS JOIN tb INNER JOIN tc ON ta.k < tc.k AND tb.k < tc.k;

-- A `WHERE` predicate merged into the join graph refers to one relation only, so it must not
-- make a pair look connected: ta and tb are linked only through tc, and the filter on tb must
-- not turn the cheap-looking ta x tb cross product into the first join.
CREATE TABLE tf (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tg (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO tf SELECT number, number % 3 FROM numbers(3);
INSERT INTO tg SELECT number FROM numbers(1000);

EXPLAIN
SELECT count()
FROM ta
INNER JOIN (SELECT tg.k AS gk FROM tf INNER JOIN tg ON tf.k = tg.k + 1 WHERE tf.x = 2) AS s ON ta.k = s.gk
SETTINGS query_plan_merge_filters_into_join = 1, query_plan_merge_expression_into_join = 1, query_plan_filter_push_down = 0;

SELECT count()
FROM ta
INNER JOIN (SELECT tg.k AS gk FROM tf INNER JOIN tg ON tf.k = tg.k + 1 WHERE tf.x = 2) AS s ON ta.k = s.gk
SETTINGS query_plan_merge_filters_into_join = 1, query_plan_merge_expression_into_join = 1, query_plan_filter_push_down = 0;
