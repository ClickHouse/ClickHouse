-- A query whose table list needs the pushed-down filters before `getQueryProcessingStage` is planned
-- twice: once over `StorageDummy` replacements to collect those filters, and then for real. The dummy
-- plan is never executed, but it used to be optimized far enough to pick a physical join, and `direct`
-- is the one algorithm that needs the right table's real storage, so the dummy replacement left it with
-- no candidate and the query failed with NOT_IMPLEMENTED. Each arm below is a way to reach that first
-- plan, and each pins the setting that reaches it rather than drawing it.

DROP TABLE IF EXISTS dj_dummy_l SYNC;
DROP TABLE IF EXISTS dj_dummy_r SYNC;

CREATE TABLE dj_dummy_l (Id UInt64) ENGINE = Memory;
CREATE TABLE dj_dummy_r (EventId UInt64, Attribute String) ENGINE = MergeTree ORDER BY EventId;
INSERT INTO dj_dummy_l SELECT number FROM numbers(1000);
INSERT INTO dj_dummy_r SELECT number, toString(number) FROM numbers(1000);

SET enable_analyzer = 1;
SET join_algorithm = 'direct';
SET query_plan_join_swap_table = 0;

-- Arm A: a non-zero `parallel_replicas_min_number_of_rows_per_replica` asks for the row estimation that
-- decides how many replicas to use, and that estimation is the consumer the filters are collected for.
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 1;
SET automatic_parallel_replicas_mode = 0;

SELECT 'count', count() FROM dj_dummy_l AS t0 INNER JOIN dj_dummy_r AS t1 ON t1.EventId = t0.Id;

-- The direct lookup is still chosen, so the row above is not correct merely because the join fell back
-- to another algorithm.
SELECT 'direct join used', countIf(explain LIKE '%DirectKeyValueJoin%') > 0
FROM (EXPLAIN optimize = 1, description = 0
      SELECT count() FROM dj_dummy_l AS t0 INNER JOIN dj_dummy_r AS t1 ON t1.EventId = t0.Id);

DROP TABLE IF EXISTS dj_dummy_dist_l SYNC;
DROP TABLE IF EXISTS dj_dummy_dist_l_local SYNC;
DROP TABLE IF EXISTS dj_dummy_dist_r SYNC;

CREATE TABLE dj_dummy_dist_l_local (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dj_dummy_dist_l AS dj_dummy_dist_l_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), dj_dummy_dist_l_local);
CREATE TABLE dj_dummy_dist_r (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO dj_dummy_dist_l_local SELECT number FROM numbers(100);
INSERT INTO dj_dummy_dist_r SELECT number, toString(number) FROM numbers(1000);

-- Arm B: a `Distributed` table asks for the same filters to skip unused shards, so this arm reaches the
-- first plan with parallel replicas off entirely. The two parallel-replicas settings are pinned per
-- query because the arm above leaves them on in the session, and the `ParallelReplicas` lane turns them
-- on server-side. `prefer_localhost_replica` decides where the join runs: at 0 the shard executes it and
-- the initiator's plan holds a remote read instead, which the control below cannot look inside.
SELECT 'distributed count', count() FROM dj_dummy_dist_l AS t0 INNER JOIN dj_dummy_dist_r AS t1 ON t1.k = t0.k
SETTINGS enable_parallel_replicas = 0, parallel_replicas_min_number_of_rows_per_replica = 0,
         prefer_localhost_replica = 1;

SELECT 'distributed direct join used', countIf(explain LIKE '%DirectKeyValueJoin%') > 0
FROM (EXPLAIN optimize = 1, description = 0
      SELECT count() FROM dj_dummy_dist_l AS t0 INNER JOIN dj_dummy_dist_r AS t1 ON t1.k = t0.k)
SETTINGS enable_parallel_replicas = 0, parallel_replicas_min_number_of_rows_per_replica = 0,
         prefer_localhost_replica = 1;

-- Arm D reaches that first plan in plan-based parallel-replicas mode, where the conversion happens at a
-- different call site: with the mode on, the traversal that would convert the joins leaves them logical
-- for `applyParallelReplicas` and a deferred pass converts whatever is left afterwards, so the arms above
-- only ever reach the first site. The mode reaches the dummy plan because that plan's optimization
-- settings come from the original query context, not from the copy its own tree carries.
SELECT 'plan-based count', count() FROM dj_dummy_l AS t0 INNER JOIN dj_dummy_r AS t1 ON t1.EventId = t0.Id
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
         parallel_replicas_plan_based = 1, parallel_replicas_min_number_of_rows_per_replica = 1;

SELECT 'plan-based direct join used', countIf(explain LIKE '%DirectKeyValueJoin%') > 0
FROM (EXPLAIN optimize = 1, description = 0
      SELECT count() FROM dj_dummy_l AS t0 INNER JOIN dj_dummy_r AS t1 ON t1.EventId = t0.Id)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
         parallel_replicas_plan_based = 1, parallel_replicas_min_number_of_rows_per_replica = 1;

DROP TABLE IF EXISTS dj_dummy_est_l SYNC;
DROP TABLE IF EXISTS dj_dummy_est_r SYNC;

CREATE TABLE dj_dummy_est_l (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 8192;
CREATE TABLE dj_dummy_est_r (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO dj_dummy_est_l SELECT number FROM numbers(1000000);
INSERT INTO dj_dummy_est_r SELECT number FROM numbers(1000);

-- Arm C is the other direction: keeping that first plan's joins logical must not stop the filters being
-- collected from it, and the estimation arm A reaches is what reads them. A selective filter on the left
-- table leaves one granule, which is less than one replica's worth of work, so the estimation turns
-- parallel replicas off; with no collected filter it would see the whole table and keep them on. Unlike
-- the arms above, this holds before the fix as well. `hash` because `direct` throws here without the fix;
-- MergeTree on the left because an `ALL INNER JOIN` gates the estimation on the left table's storage; and
-- the second row is what makes the first non-vacuous, which alone would hold wherever replicas never run.
SELECT sum(l.key) FROM dj_dummy_est_l AS l INNER JOIN dj_dummy_est_r AS r ON r.key = l.key
WHERE l.key < 1000
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
         parallel_replicas_min_number_of_rows_per_replica = 200000, parallel_replicas_local_plan = 0,
         use_query_condition_cache = 0, join_algorithm = 'hash',
         log_comment = '05233_filtered_join' FORMAT Null;

SELECT sum(l.key) FROM dj_dummy_est_l AS l INNER JOIN dj_dummy_est_r AS r ON r.key = l.key
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
         parallel_replicas_min_number_of_rows_per_replica = 200000, parallel_replicas_local_plan = 0,
         use_query_condition_cache = 0, join_algorithm = 'hash',
         log_comment = '05233_unfiltered_join' FORMAT Null;

-- The same pair in plan-based mode, which is the one mode where `applyParallelReplicas` runs on the
-- filter-collection plan too. It cannot rewrite that plan: it only acts through split markers, and those
-- are planted above `MergeTree` reads, of which the dummy replacement leaves none. These two rows are
-- what would notice if that ever stopped holding.
SELECT sum(l.key) FROM dj_dummy_est_l AS l INNER JOIN dj_dummy_est_r AS r ON r.key = l.key
WHERE l.key < 1000
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
         parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0,
         parallel_replicas_min_number_of_rows_per_replica = 200000,
         use_query_condition_cache = 0, join_algorithm = 'hash',
         log_comment = '05233_pb_filtered_join' FORMAT Null;

SELECT sum(l.key) FROM dj_dummy_est_l AS l INNER JOIN dj_dummy_est_r AS r ON r.key = l.key
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
         parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0,
         parallel_replicas_min_number_of_rows_per_replica = 200000,
         use_query_condition_cache = 0, join_algorithm = 'hash',
         log_comment = '05233_pb_unfiltered_join' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'filtered join estimate is selective', ProfileEvents['ParallelReplicasUsedCount'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '05233_filtered_join'
SETTINGS enable_parallel_replicas = 0;

SELECT 'unfiltered join uses replicas', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '05233_unfiltered_join'
SETTINGS enable_parallel_replicas = 0;

SELECT 'plan-based filtered join estimate is selective', ProfileEvents['ParallelReplicasUsedCount'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '05233_pb_filtered_join'
SETTINGS enable_parallel_replicas = 0;

SELECT 'plan-based unfiltered join uses replicas', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '05233_pb_unfiltered_join'
SETTINGS enable_parallel_replicas = 0;

DROP TABLE dj_dummy_l SYNC;
DROP TABLE dj_dummy_r SYNC;
DROP TABLE dj_dummy_dist_l SYNC;
DROP TABLE dj_dummy_dist_l_local SYNC;
DROP TABLE dj_dummy_dist_r SYNC;
DROP TABLE dj_dummy_est_l SYNC;
DROP TABLE dj_dummy_est_r SYNC;
