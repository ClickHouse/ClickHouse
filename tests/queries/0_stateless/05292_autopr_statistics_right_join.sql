-- Coverage of the physical `RIGHT`-kind path through AutoPR. **This test passes on master too** - it is not
-- a regression test for anything, and measured 3/3 green against master's `considerEnablingParallelReplicas.cpp`.
--
-- Why it cannot be one. The plan hash the two plans are matched on folds `A RIGHT JOIN B` and
-- `B LEFT JOIN A` together: `calculateHashTableCacheKeys` swaps a `RIGHT` join's children and rewrites the
-- kind to `LEFT`. So a walk that pairs the two plans by plan-child position has to pair children in that
-- same canonical order, which `collectCoordinatedReads` does through `canonicalChild`. Separating that from
-- the old kind-based rule needs the single-node plan and the replicas plan to settle on opposite spellings
-- of one join, and both are built from the same query - eight shapes were probed with the divergence itself
-- instrumented (primary-key filters on either side, each swap setting) and none diverged. On the shape below
-- the two rules agree: with the swap pinned off the kind is `RIGHT`, so `children[isRight(kind) ? 1 : 0]`
-- picks child 1, and that is also where the marker is, so nothing here tells them apart.
--
-- What it does guard, against future breakage rather than a past bug:
--   * a `RIGHT`-kind plan reaches the cost model instead of being skipped;
--   * the two spellings still fold to one hash - `kind_left_reused_them` below is the only test of that,
--     and that fold is the premise `canonicalChild` exists to respect;
--   * the second spelling adopts parallel replicas on the first one's statistics, and answers correctly.
--
-- The tests that do fail on master are `05262` and `05291`, both 6/6.
--
-- Note the two spellings cannot be compared by recorded bytes the way `05262` compares an `INNER` join:
-- they fold to one hash, so only the first of them collects and the other reuses the entry.

DROP TABLE IF EXISTS t_right_small;
DROP TABLE IF EXISTS t_right_big;
DROP TABLE IF EXISTS t_right_baseline;

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

CREATE TABLE t_right_baseline (c UInt64) ENGINE = Memory;

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

-- What the query answers without parallel replicas, so the adopted plan can be checked against it.
INSERT INTO t_right_baseline
SELECT sum(b.v) FROM t_right_small AS s RIGHT JOIN t_right_big AS b ON s.key = b.key
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

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
-- above wrote, so it costs the plan on those statistics instead of collecting its own, and it answers the
-- query so that a plan built around the wrong read shows up as a wrong result.
SELECT 'apply_kind_left', sum(b.v) = (SELECT c FROM t_right_baseline)
FROM t_right_small AS s RIGHT JOIN t_right_big AS b ON s.key = b.key
SETTINGS query_plan_join_swap_table = 'auto', log_comment = 'right_join_kind_left';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- One row per `log_comment`, the newest, so a retry into the same database cannot answer for an older run.
--
-- `kind_left_reused_them` is what says the two spellings fold to one hash: the second run finds the entry
-- the first one wrote instead of collecting its own. That fold is the reason the walk has to pair a join's
-- children canonically, so if it ever stops holding this is the test that should notice.
--
-- `kind_left_used_replicas` is what makes the second run count for something. Without it a walk that
-- refused to pair the cached `LEFT` spelling would skip the optimization silently, and the first run's
-- statistics would still be there to keep the assertion above green.
SELECT
    argMaxIf(input_bytes, event_time_microseconds, log_comment = 'right_join_kind_right') > 0
        AS right_kind_plan_collected_statistics,
    argMaxIf(input_bytes, event_time_microseconds, log_comment = 'right_join_kind_left') = 0
        AS kind_left_reused_them,
    argMaxIf(replicas_used, event_time_microseconds, log_comment = 'right_join_kind_left') > 0
        AS kind_left_used_replicas
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
      AND log_comment IN ('right_join_kind_right', 'right_join_kind_left')
)
FORMAT TSVWithNames;

DROP TABLE t_right_small;
DROP TABLE t_right_big;
DROP TABLE t_right_baseline;
