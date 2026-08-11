-- Tags: no-random-merge-tree-settings

-- Top-K dynamic filtering under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- `tryOptimizeTopK` injects an internal `__topKFilter` function that is created on demand with a runtime
-- threshold tracker and is never registered in `FunctionFactory`. Plan-based parallel replicas serializes a
-- fragment of the plan to the replicas, so a `__topKFilter` left in that fragment made the replica fail with
-- `Unknown function __topKFilter`. The optimization is now suppressed while the fragment can still be cut,
-- and re-applied per replica afterwards.
--
-- The trigger is an `ORDER BY ... LIMIT` on a column that is *not* the primary key, which is what makes
-- dynamic filtering worthwhile in the first place.

DROP TABLE IF EXISTS t_pr_top_k;

CREATE TABLE t_pr_top_k (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;

INSERT INTO t_pr_top_k SELECT number, number % 1000 FROM numbers(100000);
OPTIMIZE TABLE t_pr_top_k FINAL;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET use_top_k_dynamic_filtering = 1;
-- Pin the manual mode: CI randomizes `automatic_parallel_replicas_mode` to 2, and the cost model may then
-- decide against parallel replicas, so the plan-based split would never engage.
SET automatic_parallel_replicas_mode = 0;

-- Sorting on a non-primary-key column: this threw `Unknown function __topKFilter` on the replica. Results
-- must match non-parallel execution, in both directions.
SELECT 'asc',  b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'asc',  b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 SETTINGS parallel_replicas_plan_based = 1;
SELECT 'desc', b, a FROM t_pr_top_k ORDER BY b DESC, a DESC LIMIT 5 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'desc', b, a FROM t_pr_top_k ORDER BY b DESC, a DESC LIMIT 5 SETTINGS parallel_replicas_plan_based = 1;

-- A deep OFFSET: per-replica Top-K filtering must not drop a row that belongs in the global window.
SELECT 'offset', b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 OFFSET 4997 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'offset', b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 OFFSET 4997 SETTINGS parallel_replicas_plan_based = 1;

-- With a filter, so the Top-K filter has to coexist with a real predicate in the same read.
SELECT 'where', b, a FROM t_pr_top_k WHERE a % 7 = 0 ORDER BY b, a LIMIT 5 SETTINGS parallel_replicas_plan_based = 0;
SELECT 'where', b, a FROM t_pr_top_k WHERE a % 7 = 0 ORDER BY b, a LIMIT 5 SETTINGS parallel_replicas_plan_based = 1;

-- Skip-index-on-data-read Top-K uses the same shared threshold tracker, so exercise it too.
SELECT 'skip idx', b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5
SETTINGS parallel_replicas_plan_based = 1, use_skip_indexes_on_data_read = 1, use_skip_indexes_for_top_k = 1;

DROP TABLE t_pr_top_k;
