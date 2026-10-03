-- The cost model divides the instrumented read's `input_bytes` by the replica count, so the read it
-- measures has to be the read parallel replicas actually coordinate. Which table that is, is not the
-- optimization's decision - it only has to stay in step with the decision that was made. It used to derive
-- the read instead, by descending the query *plan* to a join's probe side
-- (`children[isRight(kind) ? 1 : 0]`), while the read being coordinated on this path is pinned to the
-- left-most table expression of the query *tree* (`findTableForParallelReplicas`). A join that swaps its
-- sides moves the first without moving the second, and the model then priced a table nobody splits - at
-- sf=100 that adopted a plan 11% slower on TPC-H q07 and declined one 62% faster on SSB q2.x.
--
-- `query_plan_join_swap_table` is the knob that moves the plan side without touching the query text, so
-- the same query is run with the swap off and forced on. The bytes recorded must be the same either way:
-- the coordinated table does not change, so neither should the statistics. Before the fix the forced-swap
-- run measured the other table instead, and the two disagreed.

DROP TABLE IF EXISTS t_coord_big;
DROP TABLE IF EXISTS t_coord_small;
DROP TABLE IF EXISTS t_coord_baseline;

-- Pinned layout: the cost model works off estimated bytes, and leaving granularity or part format to
-- randomization changes whether the optimization runs at all.
CREATE TABLE t_coord_big   (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_coord_small (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- The two tables differ by two orders of magnitude so that which one is "big" is never in doubt.
INSERT INTO t_coord_big   SELECT number, number FROM numbers(300000);
INSERT INTO t_coord_small SELECT number, number FROM numbers(3000);

OPTIMIZE TABLE t_coord_big FINAL;
OPTIMIZE TABLE t_coord_small FINAL;

CREATE TABLE t_coord_baseline (c UInt64) ENGINE = Memory;

SET enable_analyzer = 1;
-- The read has to be pruned by its own key condition only: a filter built from the other side at runtime
-- would change how many bytes are read and make the comparison below about something else.
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
-- The swap below is applied as part of join-order optimization, so it needs that optimization enabled:
-- randomization sets the limit to 0 and the two runs then no longer differ in the way this test relies on.
-- Diagnosed with `clickhouse-test --diagnose-random-settings`, minimized to
-- `query_plan_optimize_join_order_limit 0`.
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

-- What the query answers without parallel replicas, so the adopted plan can be checked against it.
INSERT INTO t_coord_baseline
SELECT sum(b.v) FROM t_coord_small AS s, t_coord_big AS b WHERE s.key = b.key AND b.key < 200000
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

-- Query-based parallel replicas only. With `parallel_replicas_plan_based = 1` the coordinated read is
-- chosen differently: `collectReadsToDistribute` descends a `JoinStepLogical` by `coordinatedJoinSide` and
-- takes `children.at(side)` of the *post-swap* children, so the swap genuinely moves which relation is
-- distributed and the two runs below would measure different reads - correctly. The invariant asserted here
-- is that the coordinated read does not move, which holds only where the query tree pins it. Pinned rather
-- than left to the default so that a change of default, or randomization of it, cannot turn that into a
-- confusing failure. `05293` covers the plan-based path.
SET parallel_replicas_plan_based = 0;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The small table is written first, so it is the one parallel replicas coordinates in both runs.
-- Each run is the first of its plan shape, which is when the statistics are collected.
SELECT sum(b.v) FROM t_coord_small AS s, t_coord_big AS b WHERE s.key = b.key AND b.key < 200000
FORMAT Null SETTINGS query_plan_join_swap_table = 'false', log_comment = 'coord_read_no_swap';

SELECT sum(b.v) FROM t_coord_small AS s, t_coord_big AS b WHERE s.key = b.key AND b.key < 200000
FORMAT Null SETTINGS query_plan_join_swap_table = 'true', log_comment = 'coord_read_forced_swap';

-- Second run of each shape. The statistics are in the cache now, so these reach the cost model and adopt
-- the plan the runs above measured - which is what makes the measurement matter. Each one also answers the
-- query, so a plan built around the wrong read shows up as a wrong result and not only as a wrong estimate.
SELECT 'apply_no_swap', sum(b.v) = (SELECT c FROM t_coord_baseline)
FROM t_coord_small AS s, t_coord_big AS b WHERE s.key = b.key AND b.key < 200000
SETTINGS query_plan_join_swap_table = 'false', log_comment = 'coord_read_apply_no_swap';

SELECT 'apply_forced_swap', sum(b.v) = (SELECT c FROM t_coord_baseline)
FROM t_coord_small AS s, t_coord_big AS b WHERE s.key = b.key AND b.key < 200000
SETTINGS query_plan_join_swap_table = 'true', log_comment = 'coord_read_apply_forced_swap';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- `both_collected_statistics` is what keeps this honest: without it two zeroes would compare equal and the
-- test would pass while measuring nothing.
-- One row per `log_comment`, the newest: a retry re-runs the queries into the same database, and the
-- count below is exact, so older rows of the same run would make it fail (or pass) for the wrong reason.
SELECT
    countIf(input_bytes > 0) = 2 AS both_collected_statistics,
    uniqExact(input_bytes) = 1 AS same_read_measured_either_way
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], event_time_microseconds) AS input_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
      AND log_comment IN ('coord_read_no_swap', 'coord_read_forced_swap')
    GROUP BY log_comment
)
FORMAT TSVWithNames;

-- Without this the two runs above could have answered correctly by not using parallel replicas at all.
SELECT countIf(replicas_used > 0) = 2 AS both_apply_runs_used_replicas
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['ParallelReplicasUsedCount'], event_time_microseconds) AS replicas_used
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
      AND log_comment IN ('coord_read_apply_no_swap', 'coord_read_apply_forced_swap')
    GROUP BY log_comment
)
FORMAT TSVWithNames;

DROP TABLE t_coord_big;
DROP TABLE t_coord_small;
DROP TABLE t_coord_baseline;
