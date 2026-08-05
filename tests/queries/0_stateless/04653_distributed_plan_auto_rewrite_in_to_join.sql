-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- make_distributed_plan auto-enables rewrite_in_to_join (issue #109476): distributed plans support
-- only some kinds of IN (subquery); the IN -> JOIN rewrite makes them distributable. The plan-shape
-- probes below fail without the auto-switch even if the runtime query happens to succeed.

DROP TABLE IF EXISTS t_in_main;
DROP TABLE IF EXISTS t_in_filter;

CREATE TABLE t_in_main (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_in_filter (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_in_main SELECT number, number * 2 FROM numbers(100000);
INSERT INTO t_in_filter SELECT number * 7 FROM numbers(1000);

-- max_rows_to_group_by must be 0: distributed aggregation cannot enforce a global limit and the
-- functional-test profile can set it nonzero. rewrite_in_to_join is deliberately NOT set here:
-- the auto-switch must enable it. The rewrite is auto-enabled only when
-- allow_experimental_correlated_subqueries is on, so pin it against randomization.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_default_shuffle_join_bucket_count = 3, max_rows_to_group_by = 0,
    enable_join_runtime_filters = 0, allow_experimental_correlated_subqueries = 1;
-- Pinned because the test greps the plan.
SET explain_query_plan_default = 'legacy';

SELECT 'IN (subquery) is rewritten to a JOIN';
-- The probes must not aggregate over EXPLAIN: an aggregating query over EXPLAIN is itself distributed.
SELECT 'rewritten to join'
FROM (EXPLAIN SELECT count() FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter))
WHERE explain ILIKE '%Join%' LIMIT 1;
-- Must produce no row: without the rewrite the plan builds the IN set via CreatingSet(s).
SELECT 'still builds sets'
FROM (EXPLAIN SELECT count() FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter))
WHERE explain ILIKE '%CreatingSet%' LIMIT 1;

SELECT 'distributed result matches single-node';
SELECT count(), sum(v) FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter);
SELECT count(), sum(v) FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter)
    SETTINGS make_distributed_plan = 0, rewrite_in_to_join = 0;
SELECT count() FROM t_in_main WHERE id NOT IN (SELECT id FROM t_in_filter);
SELECT count() FROM t_in_main WHERE id NOT IN (SELECT id FROM t_in_filter)
    SETTINGS make_distributed_plan = 0, rewrite_in_to_join = 0;

SELECT 'the query distributes';
SELECT 'distributes'
FROM (EXPLAIN PIPELINE SELECT count() FROM t_in_main WHERE id IN (SELECT id FROM t_in_filter))
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1;

DROP TABLE t_in_main;
DROP TABLE t_in_filter;
