-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for wrong results with make_distributed_plan = 1 (follow-up to issue #107946):
-- reading a Merge table over a view that joins a big table with a small (broadcast-able) table
-- returned every row once per join bucket. The second run of the distributed plan transforms on
-- the child plan stacked a ScatterExchange on top of the BroadcastExchange, re-distributing the
-- broadcast copies as if they were a partition. The transforms now run once per plan, and
-- makeDistributedPlan rejects any exchange over broadcast copies.

DROP TABLE IF EXISTS merge04613;
DROP VIEW IF EXISTS join04613;
DROP TABLE IF EXISTS big04613;
DROP TABLE IF EXISTS small04613;
DROP TABLE IF EXISTS result04613;

CREATE TABLE big04613 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
CREATE TABLE small04613 (key Int) ENGINE = MergeTree ORDER BY key;
INSERT INTO big04613 SELECT number, number * 10 FROM numbers(100000);
INSERT INTO small04613 SELECT number FROM numbers(5);

CREATE VIEW join04613 AS SELECT big04613.key AS key, big04613.value AS value
    FROM big04613 JOIN small04613 ON big04613.key = small04613.key;
CREATE TABLE merge04613 ENGINE = Merge(currentDatabase(), '^join04613$');

-- distributed_plan_max_rows_to_broadcast = 1000 pins the join strategy: the small side (5 rows) is
-- broadcast, the big side (100000 rows) gets a bucketed read. query_plan_join_swap_table = 0 keeps
-- the small table on the broadcast side. distributed_plan_default_shuffle_join_bucket_count > 1
-- pins the number of broadcast copies. The rest is pinned for the same reasons as in
-- 04367_distributed_plan_merge_scatter_multishard.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    use_statistics = 1, distributed_plan_optimize_exchanges = 1, enable_join_runtime_filters = 0,
    max_rows_to_group_by = 0, prefer_localhost_replica = 1, distributed_plan_max_rows_to_broadcast = 1000,
    distributed_plan_default_shuffle_join_bucket_count = 8, query_plan_join_swap_table = 0,
    explain_query_plan_default = 'legacy';

-- The view's plan is also the plan every Merge child builds: the small side is broadcast to every
-- join bucket. If this shape changes, the queries below may stop covering the broadcast path.
EXPLAIN SELECT key, value FROM join04613;

-- Store the result via a plain read: an ORDER BY or aggregation on top of the Merge read would be
-- distributed itself and fail on the non-serializable ReadFromMerge step (Code 344), so verify the
-- counts on a normal table instead.
CREATE TABLE result04613 (key Int, value Int) ENGINE = Memory;
INSERT INTO result04613 SELECT key, value FROM merge04613;

SET make_distributed_plan = 0;

-- Every joined row must appear exactly once: 5 rows, 5 distinct keys.
SELECT count(), uniqExact(key) FROM result04613;
SELECT key, value FROM result04613 ORDER BY key;

DROP TABLE merge04613;
DROP VIEW join04613;
DROP TABLE big04613;
DROP TABLE small04613;
DROP TABLE result04613;
