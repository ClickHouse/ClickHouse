-- Coverage of the statistics path under `parallel_replicas_plan_based = 1`. **This test passes on master
-- too** - measured 3/3 green against master's `considerEnablingParallelReplicas.cpp` - so it is coverage,
-- not a regression test. The tests that fail on master are `05262` and `05291`.
--
-- The same statistics path under `parallel_replicas_plan_based = 1`, which recognizes a different step
-- (`ReadFromParallelReplicasStep`) and marks the coordinated read in a different place
-- (`applyParallelReplicas` / `collectReadsToDistribute`, rather than `ParallelReplicasLocalPlan`, which on
-- this path only respects a mark that is already there). `collectCoordinatedReads` reads the same marker
-- either way, so it should work on both, and this is what says so.
--
-- What it cannot assert is `05262`'s invariant that the coordinated read does not move when the join swaps.
-- That is a property of the query-based path, where the query tree pins the read. Here
-- `collectReadsToDistribute` descends a `JoinStepLogical` by `coordinatedJoinSide` and takes
-- `children.at(side)` of the post-swap children, so swapping the join legitimately changes which relation
-- is distributed - measured as 12025 bytes against 1640392 on `05262`'s data, both correct for their run.
--
-- So it asserts what is invariant on this path: statistics are collected for the shape, the second run
-- reaches the cost model on them and adopts parallel replicas, and the adopted plan answers the query the
-- way the same query answers it without parallel replicas.

DROP TABLE IF EXISTS t_plan_based_big;
DROP TABLE IF EXISTS t_plan_based_small;
DROP TABLE IF EXISTS t_plan_based_baseline;

-- Pinned layout: the cost model works off estimated bytes, and leaving granularity or part format to
-- randomization changes whether the optimization runs at all.
CREATE TABLE t_plan_based_big   (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_plan_based_small (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_plan_based_big   SELECT number, number FROM numbers(300000);
INSERT INTO t_plan_based_small SELECT number, number FROM numbers(3000);

OPTIMIZE TABLE t_plan_based_big FINAL;
OPTIMIZE TABLE t_plan_based_small FINAL;

CREATE TABLE t_plan_based_baseline (c UInt64) ENGINE = Memory;

SET enable_analyzer = 1;
-- The read has to be pruned by its own key condition only: a filter built from the other side at runtime
-- would change how many bytes are read.
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

-- What the query answers without parallel replicas, so the adopted plan can be checked against it.
INSERT INTO t_plan_based_baseline
SELECT sum(b.v) FROM t_plan_based_small AS s, t_plan_based_big AS b WHERE s.key = b.key AND b.key < 200000
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SET parallel_replicas_plan_based = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- First run of the shape, which is when the statistics are collected.
SELECT sum(b.v) FROM t_plan_based_small AS s, t_plan_based_big AS b WHERE s.key = b.key AND b.key < 200000
FORMAT Null SETTINGS log_comment = 'plan_based_collect';

-- Second run. The statistics are in the cache now, so this reaches the cost model, adopts the plan built
-- around the read they describe, and answers the query.
SELECT 'plan_based_matches_single_node', sum(b.v) = (SELECT c FROM t_plan_based_baseline)
FROM t_plan_based_small AS s, t_plan_based_big AS b WHERE s.key = b.key AND b.key < 200000
SETTINGS log_comment = 'plan_based_apply';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_plan_based = 0;

SYSTEM FLUSH LOGS query_log;

-- One row per `log_comment`, the newest, so a retry into the same database cannot answer for an older run.
SELECT
    argMaxIf(input_bytes, event_time_microseconds, log_comment = 'plan_based_collect') > 0
        AS collected_statistics,
    argMaxIf(replicas_used, event_time_microseconds, log_comment = 'plan_based_apply') > 0
        AS second_run_used_replicas
FROM
(
    SELECT
        log_comment,
        event_time_microseconds,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes,
        ProfileEvents['ParallelReplicasUsedCount'] AS replicas_used
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
      AND log_comment IN ('plan_based_collect', 'plan_based_apply')
)
FORMAT TSVWithNames;

DROP TABLE t_plan_based_big;
DROP TABLE t_plan_based_small;
DROP TABLE t_plan_based_baseline;
