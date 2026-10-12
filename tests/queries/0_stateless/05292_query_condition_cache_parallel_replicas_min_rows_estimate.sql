-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that a repeated query whose parallel-replicas read is sized by `parallel_replicas_min_number_of_rows_per_replica`
-- still skips, on its second run, the granules the query condition cache recorded as not matching. The condition is on
-- a column outside the primary key, so only the cache can prune it, and the query also reads `k`, so it moves to PREWHERE.

SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
-- Only the measured queries below read with parallel replicas.
SET enable_parallel_replicas = 0;
-- The test server's profile sets a row limit; the default, no limit, is the case this test is about.
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (k Int32, v Int64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT number, number FROM numbers(500000) SETTINGS max_insert_threads = 1;

SELECT '-- parallel replicas sized by a row estimate must prune the second run';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT sum(k) FROM tab WHERE v = 7
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 1,
    log_comment = 'qcc_pr_minrows_r1';
SELECT sum(k) FROM tab WHERE v = 7
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 1,
    log_comment = 'qcc_pr_minrows_r2';
SYSTEM FLUSH LOGS query_log;
-- Run 1 populates the cache and reads every mark, run 2 hits it and reads at most two: the matching granule and, on a
-- compact part, sometimes the next one. The last value of run 2 shows that the read really went through parallel replicas.
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_pr_minrows_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] <= 2,
       ProfileEvents['IndexAnalysisRounds'],
       ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_pr_minrows_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- control: parallel replicas without a row estimate';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT sum(k) FROM tab WHERE v = 7
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0,
    log_comment = 'qcc_pr_nominrows_r1';
SELECT sum(k) FROM tab WHERE v = 7
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0,
    log_comment = 'qcc_pr_nominrows_r2';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_pr_nominrows_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] <= 2,
       ProfileEvents['IndexAnalysisRounds'],
       ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_pr_nominrows_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE tab;
