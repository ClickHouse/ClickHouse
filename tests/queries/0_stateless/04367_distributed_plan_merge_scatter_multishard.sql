-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for issue #107946: with make_distributed_plan = 1, aggregating over a Merge table
-- of Distributed tables raised the LOGICAL_ERROR exception 'ScatterExchangeStep should have one
-- source shard, got 8'. The distributed plan transforms ran twice on each Merge child plan (when
-- the child plan is created and again when its pipeline is built), and the second pass stacked
-- another ScatterExchange on top of the first pass's shuffle. The transforms now run once per plan.

DROP TABLE IF EXISTS m107946;
DROP TABLE IF EXISTS d107946_1;
DROP TABLE IF EXISTS d107946_4;
DROP TABLE IF EXISTS base107946_1;
DROP TABLE IF EXISTS base107946_4;

CREATE TABLE base107946_1 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
CREATE TABLE base107946_4 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
INSERT INTO base107946_1 SELECT number, number FROM numbers(100000);
INSERT INTO base107946_4 SELECT number, number FROM numbers(50000);

CREATE TABLE d107946_1 AS base107946_1 ENGINE = Distributed(test_shard_localhost, currentDatabase(), base107946_1);
CREATE TABLE d107946_4 AS base107946_4 ENGINE = Distributed(test_shard_localhost, currentDatabase(), base107946_4);
CREATE TABLE m107946 ENGINE = Merge(currentDatabase(), '^d107946_(1|4)$');

-- max_rows_to_group_by must be 0: the CI profile sets it to 10G and make_distributed_plan rejects
-- aggregation with a non-zero limit (Code 344).
-- prefer_localhost_replica must be 1: with 0 the Distributed child ships its plan to the localhost
-- replica over the classic protocol, which cannot deserialize distributed-plan steps (Code 47).
-- distributed_plan_max_rows_to_broadcast = 0 makes the size-based strategy choice pick shuffle
-- aggregation, and forces bucketed reads, so the child plan deterministically contains exchanges.
-- distributed_plan_default_shuffle_join_bucket_count is pinned > 1: with 1 bucket the broken path
-- is not exercised.
-- distributed_aggregation_memory_efficient must be 1: the plan below depends on the bucket-order
-- promise, which holds when either it or enable_memory_bound_merging_of_aggregation_results is on, and
-- the runner randomizes both.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    use_statistics = 1, distributed_plan_optimize_exchanges = 1, enable_join_runtime_filters = 0,
    max_rows_to_group_by = 0, prefer_localhost_replica = 1, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 8, explain_query_plan_default = 'legacy',
    distributed_aggregation_memory_efficient = 1;

-- The outer plan stays single-stage: the children aggregate up to the mergeable state themselves,
-- so there are no exchanges above ReadFromMerge. Aggregating to the mergeable state promises results
-- in bucket order, and the shuffle strategy cannot keep that promise, so each child plan carries a
-- partial aggregation and its merge around a gather rather than an aggregation over a shuffle.
EXPLAIN SELECT count(_table) FROM m107946 WHERE _table = 'base107946_1' GROUP BY _table;

-- The reproducer from the issue. It must not throw; no rows match because _table exposes the
-- underlying table names (base107946_*), same as without make_distributed_plan.
SELECT count(_table) FROM m107946 WHERE _table = 'd107946_1' GROUP BY _table;

-- Filter on the underlying table names so rows survive; the counts must match the table sizes.
SELECT count(_table) FROM m107946 WHERE _table = 'base107946_1' GROUP BY _table;
SELECT count(_table) FROM m107946 WHERE _table = 'base107946_4' GROUP BY _table;

DROP TABLE m107946;
DROP TABLE d107946_1;
DROP TABLE d107946_4;
DROP TABLE base107946_1;
DROP TABLE base107946_4;
