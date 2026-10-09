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
