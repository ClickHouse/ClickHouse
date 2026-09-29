-- A physical `RIGHT` join must reach the cost model rather than be skipped.
--
-- The plan hash the two plans are matched on folds `A RIGHT JOIN B` and `B LEFT JOIN A` together:
-- `calculateHashTableCacheKeys` swaps a `RIGHT` join's children and rewrites the kind to `LEFT`. Anything
-- that walks the two plans by plan-child position - which is how the coordinated read is paired with the
-- read to instrument - therefore has to pair children in that same canonical order, or it pairs the two
-- plans' relations the wrong way round. `collectCoordinatedReads` does that through `canonicalChild`.
--
-- What this test does NOT do: it does not fail without that remap. Reaching the mismatch needs the
-- single-node plan and the replicas plan to settle on opposite spellings of the same join, and both are
-- built from one query - eight shapes were probed (primary-key filters on either side, each swap setting)
-- and none diverged. What it does lock in is that a `RIGHT`-kind plan is handled at all, which is the
-- visible symptom if the pairing ever goes wrong: statistics are collected for the shape, and the second
-- spelling reaches the cost model on them.
--
-- Note the two spellings cannot be compared by recorded bytes the way `05262` compares an `INNER` join:
-- they fold to one hash, so only the first of them collects and the other reuses the entry.

DROP TABLE IF EXISTS t_right_small;
DROP TABLE IF EXISTS t_right_big;

-- Pinned layout: the cost model works off estimated bytes, and leaving granularity or part format to
-- randomization changes whether the optimization runs at all.
CREATE TABLE t_right_small (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_right_big   (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_right_small SELECT number, number FROM numbers(20000);
INSERT INTO t_right_big   SELECT number, number FROM numbers(400000);

OPTIMIZE TABLE t_right_small FINAL;
OPTIMIZE TABLE t_right_big FINAL;

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
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

-- `query_plan_join_swap_table = 'false'` is what keeps the physical step at kind `RIGHT`: left to itself
-- the planner rewrites `s RIGHT JOIN b` into `b LEFT JOIN s` by swapping the children. This run is the
-- first of its plan shape, which is when statistics are collected.
SELECT sum(b.v) FROM t_right_small AS s RIGHT JOIN t_right_big AS b ON s.key = b.key
FORMAT Null SETTINGS query_plan_join_swap_table = 'false', log_comment = 'right_join_kind_right';

-- The same query the planner's own way, which is the `LEFT` spelling of it. It folds to the hash the run
-- above wrote, so it costs the plan on those statistics instead of collecting its own.
SELECT sum(b.v) FROM t_right_small AS s RIGHT JOIN t_right_big AS b ON s.key = b.key
FORMAT Null SETTINGS query_plan_join_swap_table = 'auto', log_comment = 'right_join_kind_left';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- One row per `log_comment`, the newest, so a retry into the same database cannot answer for an older run.
SELECT
    input_bytes > 0 AS right_kind_plan_collected_statistics
FROM
(
    SELECT argMax(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], event_time_microseconds) AS input_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
      AND log_comment = 'right_join_kind_right'
)
FORMAT TSVWithNames;

DROP TABLE t_right_small;
DROP TABLE t_right_big;
