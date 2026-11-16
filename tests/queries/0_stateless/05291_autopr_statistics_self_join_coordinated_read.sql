-- A self-join is the sharpest case of the statistics having to follow the read parallel replicas
-- coordinate. Both sides read the same table, but only one of the two occurrences is the one whose mark
-- ranges the replicas split between them - the other is read in full on every replica - and it is the
-- split one the cost model may divide by the replica count. Matching the read by table cannot tell the two
-- apart, and inferring the side from the join kind cannot either: `JoinStepLogical::swapInputs` reorders
-- the inputs and flips `Left` <-> `Right` but leaves `Inner` alone, so a swapped `INNER` self-join looks
-- exactly like an unswapped one.
--
-- The sorting key is composite, the join uses one component and the filter the other, so the two
-- occurrences read very different amounts: the filtered one is pruned by its key condition to a fraction
-- of the table. Measuring the wrong one is therefore not a rounding error - before the fix the forced-swap
-- run recorded 25 KB where the coordinated read was 2.4 MB.

DROP TABLE IF EXISTS t_self_coord;
DROP TABLE IF EXISTS t_self_coord_baseline;

-- Pinned layout: the cost model works off estimated bytes, and leaving granularity or part format to
-- randomization changes whether the optimization runs at all.
CREATE TABLE t_self_coord (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- `a` repeats every 1000 rows and `b` is unique, so a range condition on `a` prunes by the primary key
-- while the join on `b` reads everything.
INSERT INTO t_self_coord SELECT number % 1000, number, number FROM numbers(300000);
OPTIMIZE TABLE t_self_coord FINAL;

CREATE TABLE t_self_coord_baseline (left_sum UInt64, right_sum UInt64) ENGINE = Memory;

SET enable_analyzer = 1;
-- The read has to be pruned by its own key condition only: a filter built from the other side at runtime
-- would change how many bytes are read and make the comparison below about something else.
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
-- The swap below is applied as part of join-order optimization, so it needs that optimization enabled.
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

-- What the query answers without parallel replicas, so the adopted plan can be checked against it.
INSERT INTO t_self_coord_baseline
SELECT sum(l.v), sum(r.v) FROM t_self_coord AS l INNER JOIN t_self_coord AS r ON l.b = r.b WHERE r.a < 10
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

-- `l` is the left-most table expression, so it is the occurrence parallel replicas coordinate in both
-- runs, while the filter is on `r`. Each run is the first of its plan shape, which is when the statistics
-- are collected.
SELECT sum(l.v), sum(r.v) FROM t_self_coord AS l INNER JOIN t_self_coord AS r ON l.b = r.b WHERE r.a < 10
FORMAT Null SETTINGS query_plan_join_swap_table = 'false', log_comment = 'self_coord_no_swap';

SELECT sum(l.v), sum(r.v) FROM t_self_coord AS l INNER JOIN t_self_coord AS r ON l.b = r.b WHERE r.a < 10
FORMAT Null SETTINGS query_plan_join_swap_table = 'true', log_comment = 'self_coord_forced_swap';

-- Second run of each shape. The statistics are in the cache now, so these reach the cost model and adopt
-- the plan the runs above measured. Each one also answers the query: on a self-join the two occurrences
-- read different amounts, so a plan built around the wrong one shows up as a wrong result too.
SELECT 'apply_no_swap',
       sum(l.v) = (SELECT left_sum FROM t_self_coord_baseline) AND sum(r.v) = (SELECT right_sum FROM t_self_coord_baseline)
FROM t_self_coord AS l INNER JOIN t_self_coord AS r ON l.b = r.b WHERE r.a < 10
SETTINGS query_plan_join_swap_table = 'false', log_comment = 'self_coord_apply_no_swap';

SELECT 'apply_forced_swap',
       sum(l.v) = (SELECT left_sum FROM t_self_coord_baseline) AND sum(r.v) = (SELECT right_sum FROM t_self_coord_baseline)
FROM t_self_coord AS l INNER JOIN t_self_coord AS r ON l.b = r.b WHERE r.a < 10
SETTINGS query_plan_join_swap_table = 'true', log_comment = 'self_coord_apply_forced_swap';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- `both_collected_statistics` is what keeps this honest: without it two zeroes would compare equal and the
-- test would pass while measuring nothing.
-- One row per `log_comment`, the newest: a retry re-runs the queries into the same database, and the
-- count below is exact, so older rows of the same run would make it fail (or pass) for the wrong reason.
SELECT
    countIf(input_bytes > 0) = 2 AS both_collected_statistics,
    uniqExact(input_bytes) = 1 AS same_occurrence_measured_either_way
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], event_time_microseconds) AS input_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
      AND log_comment IN ('self_coord_no_swap', 'self_coord_forced_swap')
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
      AND log_comment IN ('self_coord_apply_no_swap', 'self_coord_apply_forced_swap')
    GROUP BY log_comment
)
FORMAT TSVWithNames;

DROP TABLE t_self_coord;
DROP TABLE t_self_coord_baseline;
