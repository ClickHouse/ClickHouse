-- Tags: no-random-merge-tree-settings

-- Top-K dynamic filtering under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- `tryOptimizeTopK` injects an internal `__topKFilter` function that is created on demand with a runtime
-- threshold tracker and is never registered in `FunctionFactory`. Plan-based parallel replicas serializes a
-- fragment of the plan to the replicas, so a `__topKFilter` left in that fragment made the replica fail with
-- `Unknown function __topKFilter`. A read carrying Top-K is now kept out of the fragment instead, so the
-- query runs locally with Top-K rather than being distributed.
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
SET parallel_replicas_plan_based = 1;
SET use_top_k_dynamic_filtering = 1;
-- Pin the manual mode: CI randomizes `automatic_parallel_replicas_mode` to 2, and the cost model may then
-- decide against parallel replicas, so the plan-based split would never engage.
SET automatic_parallel_replicas_mode = 0;

-- Sorting on a non-primary-key column: this threw `Unknown function __topKFilter` on the replica. Results
-- must match non-parallel execution, in both directions.
SELECT '--- ORDER BY b, a LIMIT 5, local ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT '--- ORDER BY b, a LIMIT 5, plan_based = 1 ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5;
SELECT '--- ORDER BY b DESC, a DESC LIMIT 5, local ---';
SELECT b, a FROM t_pr_top_k ORDER BY b DESC, a DESC LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT '--- ORDER BY b DESC, a DESC LIMIT 5, plan_based = 1 ---';
SELECT b, a FROM t_pr_top_k ORDER BY b DESC, a DESC LIMIT 5;

-- A deep OFFSET: the Top-K threshold must not drop a row that belongs in the requested window.
SELECT '--- deep OFFSET, local ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 OFFSET 4997 SETTINGS enable_parallel_replicas = 0;
SELECT '--- deep OFFSET, plan_based = 1 ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 OFFSET 4997;

-- With a filter, so the Top-K filter has to coexist with a real predicate in the same read.
SELECT '--- WHERE + ORDER BY b, a LIMIT 5, local ---';
SELECT b, a FROM t_pr_top_k WHERE a % 7 = 0 ORDER BY b, a LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT '--- WHERE + ORDER BY b, a LIMIT 5, plan_based = 1 ---';
SELECT b, a FROM t_pr_top_k WHERE a % 7 = 0 ORDER BY b, a LIMIT 5;

-- Skip-index-on-data-read Top-K uses the same shared threshold tracker, so exercise it too.
SELECT '--- use_skip_indexes_on_data_read = 1, use_skip_indexes_for_top_k = 1, plan_based = 1 ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5
SETTINGS use_skip_indexes_on_data_read = 1, use_skip_indexes_for_top_k = 1;

-- The shape assertions below pin every knob that decides whether Top-K applies at all: CI randomizes
-- `use_top_k_dynamic_filtering`, `use_skip_indexes_for_top_k` and `query_plan_max_limit_for_top_k_optimization`
-- (which can be 0 or 1, both of which disable it for this query), and without Top-K the read simply
-- distributes as usual.
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;

-- Top-K wins over distributing this read: the `__topKFilter` cannot be serialized, so the read is kept out
-- of the shipped fragment and the query runs locally with Top-K still applied.
SELECT '--- explain: Top-K kept in the plan ---';
SELECT countIf(explain LIKE '%topKFilter%') > 0 AS has_top_k
FROM (EXPLAIN actions = 1 SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5);
SELECT '--- explain: read not distributed ---';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5);

-- Top-K is not lost merely because the setting is on: with Top-K disabled the same read distributes as usual,
-- so the exclusion above is scoped to reads that actually carry the filter.
SELECT '--- explain: read distributed with use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0 ---';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5)
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- That distributed read is the shipped-`SortingStep` path for a non-primary-key `ORDER BY`, so check its
-- results too: the shape assertion above still passes if the per-replica sort or the merge on the initiator
-- returns the wrong rows.
SET use_top_k_dynamic_filtering = 0;
SET use_skip_indexes_for_top_k = 0;

SELECT '--- Top-K disabled, ORDER BY b, a LIMIT 5, local ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT '--- Top-K disabled, ORDER BY b, a LIMIT 5, plan_based = 1 ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5;

-- A deep OFFSET, applied once above the merge, while a replica may stop at `LIMIT` + `OFFSET` rows.
SELECT '--- Top-K disabled, deep OFFSET, local ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 OFFSET 4997 SETTINGS enable_parallel_replicas = 0;
SELECT '--- Top-K disabled, deep OFFSET, plan_based = 1 ---';
SELECT b, a FROM t_pr_top_k ORDER BY b, a LIMIT 5 OFFSET 4997;

DROP TABLE t_pr_top_k;
