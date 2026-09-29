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

-- Pinned layout: the cost model works off estimated bytes, and leaving granularity or part format to
-- randomization changes whether the optimization runs at all.
CREATE TABLE t_self_coord (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- `a` repeats every 1000 rows and `b` is unique, so a range condition on `a` prunes by the primary key
-- while the join on `b` reads everything.
INSERT INTO t_self_coord SELECT number % 1000, number, number FROM numbers(300000);
OPTIMIZE TABLE t_self_coord FINAL;

SET enable_analyzer = 1;
-- The read has to be pruned by its own key condition only: a filter built from the other side at runtime
-- would change how many bytes are read and make the comparison below about something else.
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
-- The swap below is applied as part of join-order optimization, so it needs that optimization enabled.
SET query_plan_optimize_join_order_limit = 10;
-- Without statistics the cost model does not favour replicas for this data at all: the optimization is
-- then never applied and the test measures nothing.
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

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

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- `both_collected_statistics` is what keeps this honest: without it two zeroes would compare equal and the
-- test would pass while measuring nothing.
SELECT
    countIf(input_bytes > 0) = 2 AS both_collected_statistics,
    uniqExact(input_bytes) = 1 AS same_occurrence_measured_either_way
FROM
(
    SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
      AND event_date >= yesterday()
      AND log_comment IN ('self_coord_no_swap', 'self_coord_forced_swap')
)
FORMAT TSVWithNames;

DROP TABLE t_self_coord;
