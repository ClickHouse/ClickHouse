-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Distributed plans support `IN (subquery)` (issue #109476): the set is built once on the
-- initiator and its values ship with the worker tasks, so there is no `IN` -> `JOIN`
-- auto-rewrite. The rewrite stays available as an explicit setting.

DROP TABLE IF EXISTS t_in_main;
DROP TABLE IF EXISTS t_in_filter;

CREATE TABLE t_in_main (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_in_filter (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_in_main SELECT number, number * 2 FROM numbers(100000);
INSERT INTO t_in_filter SELECT number * 7 FROM numbers(1000);

-- max_rows_to_group_by must be 0: distributed aggregation cannot enforce a global limit and the
-- functional-test profile can set it nonzero. The index settings are pinned so the plan shape
-- does not depend on randomized settings.
SET distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0,
    enable_join_runtime_filters = 0, allow_experimental_correlated_subqueries = 1,
    use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;
-- Pinned because the test greps the plan.
SET explain_query_plan_default = 'legacy';

-- The probe wrappers run without make_distributed_plan (it is set inside the explained query):
-- a distributed wrapper would collect the explained query's IN set into its own plan.

SELECT 'IN (subquery) stays an IN and keeps its set';
-- Must produce no row: the IN is not rewritten into a join.
SELECT 'rewritten to join'
FROM (EXPLAIN SELECT count() FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter) SETTINGS make_distributed_plan = 1)
WHERE explain ILIKE '%Join%' LIMIT 1;
-- Set expansion is deferred under make_distributed_plan, so the plan keeps a delayed set step.
SELECT 'keeps the set'
FROM (EXPLAIN SELECT count() FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter) SETTINGS make_distributed_plan = 1)
WHERE explain ILIKE '%CreatingSet%' LIMIT 1;

SELECT 'the explicit rewrite still turns IN into a JOIN';
SELECT 'rewritten to join'
FROM (EXPLAIN SELECT count() FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter) SETTINGS make_distributed_plan = 1, rewrite_in_to_join = 1)
WHERE explain ILIKE '%Join%' LIMIT 1;

SELECT 'distributed result matches single-node';
SET make_distributed_plan = 1;
SELECT count(), sum(v) FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter);
SELECT count(), sum(v) FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter)
    SETTINGS make_distributed_plan = 0, rewrite_in_to_join = 0;
SELECT count() FROM t_in_main WHERE id NOT IN (SELECT id FROM t_in_filter);
SELECT count() FROM t_in_main WHERE id NOT IN (SELECT id FROM t_in_filter)
    SETTINGS make_distributed_plan = 0, rewrite_in_to_join = 0;

SELECT 'the query distributes';
SET make_distributed_plan = 0;
SELECT 'distributes'
FROM (EXPLAIN PIPELINE SELECT count() FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter) SETTINGS make_distributed_plan = 1)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

DROP TABLE t_in_main;
DROP TABLE t_in_filter;
