-- Tags: no-sanitizers
-- no-sanitizers: needs enough data and small blocks to make parallel replicas cost-beneficial,
--                which is too slow under sanitizers (as for 04341).

-- Regression test for a logical error found by the AST fuzzer while fuzzing 04341 (PR #106073):
--   Logical error: 'local_replica_plan_reading_step->getAnalyzedResult() == nullptr'
-- The AutoPR apply path transplants the single-node index analysis onto the parallel-replicas branch
-- read (to honor parallel_replicas_index_analysis_only_on_coordinator) and used to assert that branch
-- read is unanalyzed. That holds for a plain LEFT JOIN, but a top-level SELECT DISTINCT over the join
-- (plus a scalar subquery) makes the branch read already analyzed by the time AutoPR reaches it, so the
-- apply path hit the chassert in debug builds (and would silently overwrite the analysis in release).
-- The query below is the reduced fuzzer reproducer; the apply run must complete and stay correct.
-- CI report: https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=106073&sha=c691b4191826494e69ec29ab93dc3279da6aaa0f&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%29

DROP TABLE IF EXISTS aj_big;
DROP TABLE IF EXISTS aj_small;
DROP TABLE IF EXISTS aj_baseline;

CREATE TABLE aj_big (key UInt64, payload String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
CREATE TABLE aj_small (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
INSERT INTO aj_big SELECT number, toString(cityHash64(number)) FROM numbers(5e5);
INSERT INTO aj_small SELECT number * 2 FROM numbers(2.5e5);

-- Baseline with parallel replicas OFF, so it doesn't pre-collect stats (see 04341).
SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;
CREATE TABLE aj_baseline (kind String, c UInt64) ENGINE = Memory;
INSERT INTO aj_baseline SELECT 'left', sum(cityHash64(t1.payload)) FROM aj_big AS t1 LEFT JOIN aj_small AS t2 USING (key);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;
SET query_plan_join_swap_table='false';
SET enable_analyzer=1;
SET max_threads=4, max_block_size=128;
SET automatic_parallel_replicas_min_bytes_per_replica=0, merge_tree_min_bytes_per_task_for_remote_reading=0;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- The fuzzer's shape: top-level SELECT DISTINCT over a LEFT JOIN with a scalar subquery. The first run
-- collects stats; the second run applies parallel replicas and reaches the branch read that the
-- DISTINCT/subquery already analyzed (the chassert in debug). Both must return result_ok = 1.
SELECT DISTINCT (SELECT DISTINCT c FROM aj_baseline WHERE materialize('left') = kind LIMIT 1024) = sum(cityHash64(t1.payload)) AS result_ok
    FROM aj_big AS t1 LEFT JOIN aj_small AS t2 USING (key) SETTINGS log_comment='04342_distinct_collect';
SELECT DISTINCT (SELECT DISTINCT c FROM aj_baseline WHERE materialize('left') = kind LIMIT 1024) = sum(cityHash64(t1.payload)) AS result_ok
    FROM aj_big AS t1 LEFT JOIN aj_small AS t2 USING (key) SETTINGS log_comment='04342_distinct_apply';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- The apply run must have used parallel replicas (collect=0 / apply=1). The scalar subquery shares the
-- log_comment, so aggregate with max() to pick the main join query.
SELECT log_comment, max(ProfileEvents['ParallelReplicasUsedCount']) > 0 AS pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
  AND (current_database = currentDatabase()) AND (log_comment LIKE '04342_distinct_%') AND (type = 'QueryFinish')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE aj_big;
DROP TABLE aj_small;
DROP TABLE aj_baseline;
