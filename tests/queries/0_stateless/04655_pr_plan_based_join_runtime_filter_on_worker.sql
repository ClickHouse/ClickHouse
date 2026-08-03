-- A JOIN shipped by plan-based parallel replicas carries its join runtime filter into the fragment, where
-- the shipped `BuildRuntimeFilterStep` is inert: a deserialized step has no rendezvous key, so the replica
-- must re-derive the filter itself while re-optimizing the fragment. This test asserts it really does, and
-- that the filter is applied, not just planned. See PR #112268 review (comment r3685836765).
--
-- `parallel_replicas_local_plan = 0` puts the whole join on the replicas, so every runtime-filter counter
-- below can only come from them. The fragment arrives as a plan packet rather than SQL, so its `query_log`
-- row carries neither `tables` nor `log_comment` - it has to be found through `initial_query_id`, which is
-- why the join result is captured together with `queryID()`.

DROP TABLE IF EXISTS rf_probe SYNC;
DROP TABLE IF EXISTS rf_build SYNC;
DROP TABLE IF EXISTS rf_query SYNC;

CREATE TABLE rf_probe (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
CREATE TABLE rf_build (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
INSERT INTO rf_probe SELECT number FROM numbers(300000);
INSERT INTO rf_build SELECT number FROM numbers(10000);   -- 10000 of the 300000 probe keys match

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

-- RIGHT JOIN: the build side is the coordinated side, so this is the shape whose split has to be lifted
-- through `BuildRuntimeFilterStep` for the join to ship at all.
CREATE TABLE rf_query ENGINE = Memory AS
    SELECT queryID() AS query_id, count() AS cnt
    FROM rf_probe RIGHT JOIN rf_build ON rf_probe.a = rf_build.a
    SETTINGS enable_join_runtime_filters = 1;

SYSTEM FLUSH LOGS query_log;

SELECT 'result', (SELECT cnt FROM rf_query);
SELECT 'remote queries', countIf(is_initial_query = 0) >= 1,
       'filter built on worker', sumIf(ProfileEvents['RuntimeFiltersCreated'], is_initial_query = 0) > 0,
       'filter applied on worker', sumIf(ProfileEvents['RuntimeFilterRowsPassed'], is_initial_query = 0)
                                       < sumIf(ProfileEvents['RuntimeFilterRowsChecked'], is_initial_query = 0)
FROM system.query_log
WHERE initial_query_id = (SELECT query_id FROM rf_query) AND type = 'QueryFinish'
  -- `initial_query_id` already scopes this to the query above; the date and time bounds only keep the scan
  -- from touching older partitions when the test is rerun on a busy server.
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR;

-- Control: with the filter disabled the same counters must stay at zero, so the numbers above cannot come
-- from anything else.
TRUNCATE TABLE rf_query;
INSERT INTO rf_query
    SELECT queryID() AS query_id, count() AS cnt
    FROM rf_probe RIGHT JOIN rf_build ON rf_probe.a = rf_build.a
    SETTINGS enable_join_runtime_filters = 0;

SYSTEM FLUSH LOGS query_log;

SELECT 'result', (SELECT cnt FROM rf_query);
SELECT 'no filter built', sumIf(ProfileEvents['RuntimeFiltersCreated'], is_initial_query = 0) = 0
FROM system.query_log
WHERE initial_query_id = (SELECT query_id FROM rf_query) AND type = 'QueryFinish'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR;

DROP TABLE rf_probe SYNC;
DROP TABLE rf_build SYNC;
DROP TABLE rf_query SYNC;
