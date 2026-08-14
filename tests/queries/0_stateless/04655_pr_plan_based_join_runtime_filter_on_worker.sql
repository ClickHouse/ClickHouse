-- A JOIN shipped by plan-based parallel replicas carries its join runtime filter into the fragment, but the
-- shipped build step does nothing there: a deserialized step cannot publish its filter, so the replica has
-- to build its own while re-optimizing the fragment. This test asserts it really does, and that the filter is
-- applied, not just planned. See PR #112268 review (comment r3685836765).
--
-- `parallel_replicas_local_plan = 0` puts the whole join on the replicas, so every runtime-filter counter
-- below can only come from them. A fragment arrives as a plan packet rather than SQL, so its `query_log` row
-- has neither `tables` nor `log_comment`, and runs with `current_database = default` rather than the test
-- database - so it is found through `initial_query_id` of the initiator's row, which in turn is identified by
-- its database, the table it reads and the setting it ran with.

DROP TABLE IF EXISTS rf_probe SYNC;
DROP TABLE IF EXISTS rf_build SYNC;

CREATE TABLE rf_probe (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
CREATE TABLE rf_build (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
INSERT INTO rf_probe SELECT number FROM numbers(300_000);
INSERT INTO rf_build SELECT number FROM numbers(10_000);   -- 10000 of the 300000 probe keys match

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 0;
SET parallel_replicas_prefer_local_replica = 0;
SET join_runtime_filter_min_probe_rows = 1;
-- Pin the join order: a randomized order swaps the sides, and a filter is only built for a join that may
-- drop probe rows, so without this the filter is sometimes not created at all.
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
-- Keep the filter on the row-level path: with index analysis the pruning happens at granule level instead,
-- so the rows never reach `__applyFilter` and the row counters asserted below stop being comparable.
SET enable_join_runtime_filters_index_analysis = 0;
-- Never let the filter disable itself: `rf_build` holds the first 10000 probe keys and `rf_probe` is ordered
-- by the same one, so a first block of at most 10000 rows passes every row and trips the pass-ratio
-- heuristic. A skipped block is counted as `RuntimeFilterRowsSkipped` only, so it lands on neither side of
-- the `Passed < Checked` assertion below, which then depends on the randomized block size.
SET join_runtime_filter_blocks_to_skip_before_reenabling = 0;

-- RIGHT JOIN: the build side is the coordinated side, so this is the shape whose split has to be lifted
-- through `BuildRuntimeFilterStep` for the join to ship at all.
SELECT 'result', count() FROM rf_probe RIGHT JOIN rf_build ON rf_probe.a = rf_build.a
SETTINGS enable_join_runtime_filters = 1;

-- Control: the same join with the filter disabled, so the counters below cannot come from anything else.
SELECT 'result', count() FROM rf_probe RIGHT JOIN rf_build ON rf_probe.a = rf_build.a
SETTINGS enable_join_runtime_filters = 0;

SYSTEM FLUSH LOGS query_log;

SELECT 'remote queries', countIf(is_initial_query = 0) >= 1,
       'filter built on worker', sumIf(ProfileEvents['RuntimeFiltersCreated'], is_initial_query = 0) > 0,
       'filter applied on worker', sumIf(ProfileEvents['RuntimeFilterRowsPassed'], is_initial_query = 0)
                                       < sumIf(ProfileEvents['RuntimeFilterRowsChecked'], is_initial_query = 0),
       'filter never disabled', sumIf(ProfileEvents['RuntimeFilterRowsSkipped'], is_initial_query = 0) = 0
FROM system.query_log
WHERE initial_query_id = (
    SELECT query_id FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query = 1 AND current_database = currentDatabase()
      AND has(tables, currentDatabase() || '.rf_probe')
      AND Settings['enable_join_runtime_filters'] = '1'
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
    ORDER BY event_time DESC LIMIT 1)
  AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR;

SELECT 'no filter built', sumIf(ProfileEvents['RuntimeFiltersCreated'], is_initial_query = 0) = 0
FROM system.query_log
WHERE initial_query_id = (
    SELECT query_id FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query = 1 AND current_database = currentDatabase()
      AND has(tables, currentDatabase() || '.rf_probe')
      AND Settings['enable_join_runtime_filters'] = '0'
      AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
    ORDER BY event_time DESC LIMIT 1)
  AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR;

DROP TABLE rf_probe SYNC;
DROP TABLE rf_build SYNC;
