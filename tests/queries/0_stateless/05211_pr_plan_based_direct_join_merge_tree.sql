-- A direct join into a MergeTree right table moves that read out of the query plan and into a lookup
-- plan that the join transform rebuilds and executes once per key batch. Under plan-based parallel
-- replicas that per-batch pipeline was distributed across the cluster, which silently dropped matching
-- rows, so the counts below must equal the non-parallel result for both fragment execution paths.
-- The lookup read is stamped `disableQueryConditionCache` at plan time because its hand-built filter
-- has one identity per batch; a serialized read drops that stamp, so shipped batches shared one entry.
-- Two axes are pinned because the runner otherwise weakens or removes the loss:
--   * the number of key batches, which is the left table's stored block count. That count is fixed by
--     the INSERT, not by the read: StorageMemory does not prefer large blocks, so the insert squashes
--     to `max_block_size`. The runner's session value draws 8000..100000, i.e. 1..13 batches, so the
--     amount lost varies per run, and at one batch there is no earlier entry to reuse and nothing is
--     lost at all.
--   * `use_query_condition_cache`, the cache itself, which the runner turns off half the time.
-- `index_granularity` is pinned only to fix the granule count the mechanism above is described in
-- terms of; it is not load-bearing, because 100000 rows never fit in one granule at any granularity
-- the runner can draw and two granules are already enough to lose rows.

DROP TABLE IF EXISTS dj_l SYNC;
DROP TABLE IF EXISTS dj_r SYNC;

CREATE TABLE dj_l (Id UInt64) ENGINE = Memory;
CREATE TABLE dj_r (EventId UInt64, Attribute String) ENGINE = MergeTree ORDER BY EventId
    SETTINGS index_granularity = 8192;
INSERT INTO dj_l SELECT number FROM numbers(100000) SETTINGS max_block_size = 10000;  -- 10 key batches
INSERT INTO dj_r SELECT number, toString(number) FROM numbers(100000);

SET enable_analyzer = 1;
SET join_algorithm = 'direct';
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- No local read on the initiator, so every lookup batch was served by a remote replica. This is the
-- shape that lost more than four fifths of the matching rows.
SELECT 'local_plan 0', count() FROM dj_l AS t0 INNER JOIN dj_r AS t1 ON t1.EventId = t0.Id
SETTINGS parallel_replicas_local_plan = 0, use_query_condition_cache = 1,
         log_comment = '05211_direct_join_lookup';

-- The default local-plan path, which is the configuration the linked issue reports. It was already
-- correct here, and both fragment execution paths are kept so neither can regress unnoticed.
SELECT 'local_plan 1', count() FROM dj_l AS t0 INNER JOIN dj_r AS t1 ON t1.EventId = t0.Id
SETTINGS parallel_replicas_local_plan = 1, use_query_condition_cache = 1;

-- The direct lookup is still chosen, so the counts above are not correct merely because the join
-- fell back to another algorithm.
SELECT 'direct join used', countIf(explain LIKE '%DirectKeyValueJoin%') > 0
FROM (EXPLAIN optimize = 1, description = 0
      SELECT count() FROM dj_l AS t0 INNER JOIN dj_r AS t1 ON t1.EventId = t0.Id);

-- An ordinary read of the same right table with the same fragment path, as the positive control for
-- the two witnesses below. The predicate has to survive a scan-free answer: an unfiltered count is
-- served by the trivial-count optimization and `Attribute != ''` by
-- `optimize_trivial_count_with_sparsity_filter`, and neither reaches a replica.
SELECT count() FROM dj_r WHERE EventId % 3 = 1
SETTINGS parallel_replicas_local_plan = 0, log_comment = '05211_direct_join_control' FORMAT Null;

-- `ParallelReplicasUsedCount` counts the distinct replicas a reading coordinator handed ranges to, so
-- no replica served any part of the affected join while some replica served the control read. The
-- second row is what makes the first non-vacuous: on its own the first would also hold in an
-- environment where plan-based parallel replicas declined for every query, and this test would keep
-- passing while exercising nothing. Neither uses `ParallelReplicasQueryCount`, which counts
-- coordinators constructed and is 1 either way here.
SYSTEM FLUSH LOGS query_log;
SELECT 'lookup not distributed', ProfileEvents['ParallelReplicasUsedCount'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '05211_direct_join_lookup'
SETTINGS enable_parallel_replicas = 0;

SELECT 'parallel replicas live', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '05211_direct_join_control'
SETTINGS enable_parallel_replicas = 0;

DROP TABLE dj_l SYNC;
DROP TABLE dj_r SYNC;
