-- Tags: no-sanitizers, long
-- no-sanitizers: needs enough data and small blocks to collect stats and make parallel replicas
--                cost-beneficial, which is too slow under sanitizers (as for 03783 / 04034).

-- Exercises the AutoPR APPLY path for JOIN queries with automatic_parallel_replicas_mode=1:
-- findReadingStep on the parallel-replicas plan, setAnalyzedResult, and replaceNodeWithPlan. mode=2
-- only collects statistics and intentionally never applies, so the join apply path is otherwise
-- untested - a regression where stats are collected but the cached JOIN plan is never used, uses the
-- wrong local branch, or doesn't report ParallelReplicasUsedCount would slip through. For INNER,
-- LEFT and RIGHT: a first run collects stats, a second run applies parallel replicas. Assert that
-- parallel replicas were actually used AND that the result matches the non-parallel-replicas baseline.
--
-- The apply runs are TOP-LEVEL SELECTs on purpose: that is what goes through the AutoPR cost decision.
-- An INSERT ... SELECT would enable parallel replicas unconditionally (bypassing AutoPR), so it would
-- not test this path. Each apply run self-checks its result against the non-PR baseline via a scalar
-- subquery, which stays above the matched aggregation and does not disturb the parallel-replicas plan.

DROP TABLE IF EXISTS aj_big;
DROP TABLE IF EXISTS aj_small;
DROP TABLE IF EXISTS aj_baseline;

-- Small index_granularity so reads produce enough blocks for the statistics collector.
CREATE TABLE aj_big (key UInt64, payload String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
CREATE TABLE aj_small (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
INSERT INTO aj_big SELECT number, toString(cityHash64(number)) FROM numbers(5e5);
INSERT INTO aj_small SELECT number * 2 FROM numbers(2.5e5);

-- Compute baselines with parallel replicas explicitly OFF. The test harness may randomize
-- enable_parallel_replicas / automatic_parallel_replicas_mode on; if the baseline joins ran in
-- mode 2 they would collect stats for these join keys and the later `collect` run would then find
-- them and apply, breaking the collect=0 / apply=1 assertion below.
SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

-- Non-parallel-replicas baselines. `sum` is order-independent, so robust to row-order differences.
CREATE TABLE aj_baseline (kind String, c UInt64) ENGINE = Memory;
INSERT INTO aj_baseline SELECT 'inner', sum(cityHash64(t1.payload)) FROM aj_big AS t1 INNER JOIN aj_small AS t2 USING (key);
INSERT INTO aj_baseline SELECT 'left',  sum(cityHash64(t1.payload)) FROM aj_big AS t1 LEFT  JOIN aj_small AS t2 USING (key);
INSERT INTO aj_baseline SELECT 'right', sum(cityHash64(t2.payload)) FROM aj_small AS t1 RIGHT JOIN aj_big AS t2 USING (key);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;
-- Keep the parallelized side oriented as written (the randomizer may flip this).
SET query_plan_join_swap_table='false';
-- For runs with the old analyzer
SET enable_analyzer=1;
-- Small blocks so enough are fed to the statistics collector.
SET max_threads=4, max_block_size=128;
-- Don't let the min-bytes gate or remote-read task sizing disable parallel replicas for this data.
SET automatic_parallel_replicas_min_bytes_per_replica=0, merge_tree_min_bytes_per_task_for_remote_reading=0;
-- External aggregation is not supported, i.e. no statistics would be reported.
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- INNER: big left side is parallelized (child 0). First run collects, second applies + self-checks.
SELECT sum(cityHash64(t1.payload)) FROM aj_big AS t1 INNER JOIN aj_small AS t2 USING (key) FORMAT Null SETTINGS log_comment='04341_join_inner_collect';
SELECT 'inner' AS kind, sum(cityHash64(t1.payload)) = (SELECT c FROM aj_baseline WHERE kind='inner') AS result_ok
    FROM aj_big AS t1 INNER JOIN aj_small AS t2 USING (key) SETTINGS log_comment='04341_join_inner_apply';

-- LEFT: big left side is parallelized (child 0).
SELECT sum(cityHash64(t1.payload)) FROM aj_big AS t1 LEFT JOIN aj_small AS t2 USING (key) FORMAT Null SETTINGS log_comment='04341_join_left_collect';
SELECT 'left' AS kind, sum(cityHash64(t1.payload)) = (SELECT c FROM aj_baseline WHERE kind='left') AS result_ok
    FROM aj_big AS t1 LEFT JOIN aj_small AS t2 USING (key) SETTINGS log_comment='04341_join_left_apply';

-- RIGHT: big table on the right is parallelized (child 1).
SELECT sum(cityHash64(t2.payload)) FROM aj_small AS t1 RIGHT JOIN aj_big AS t2 USING (key) FORMAT Null SETTINGS log_comment='04341_join_right_collect';
SELECT 'right' AS kind, sum(cityHash64(t2.payload)) = (SELECT c FROM aj_baseline WHERE kind='right') AS result_ok
    FROM aj_small AS t1 RIGHT JOIN aj_big AS t2 USING (key) SETTINGS log_comment='04341_join_right_apply';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Correctness was checked inline above (each apply printed `<kind> 1`). Now assert the AutoPR cost
-- decision itself: the first (collect) run must NOT use parallel replicas (no stats yet), and the
-- second (apply) run must. That collect=0 / apply=1 pattern is what distinguishes the AutoPR algorithm
-- from unconditional application - if parallel replicas were applied unconditionally, the collect run
-- would also report 1.
SELECT log_comment, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
  AND (current_database = currentDatabase()) AND (log_comment LIKE '04341_join_%') AND (type = 'QueryFinish')
ORDER BY log_comment;

DROP TABLE aj_big;
DROP TABLE aj_small;
DROP TABLE aj_baseline;
