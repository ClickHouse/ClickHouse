-- Tags: no-replicated-database
-- - no-replicated-database - the test reads through a two-replica cluster, while a Replicated
--   database test uses a different cluster

DROP TABLE IF EXISTS t_pr_in_set;
CREATE TABLE t_pr_in_set (key Int, value Int) ENGINE = MergeTree() ORDER BY key;
SYSTEM STOP MERGES t_pr_in_set;
INSERT INTO t_pr_in_set SELECT number, number * 100 FROM numbers(100000)
SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;

SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    automatic_parallel_replicas_mode = 0, use_query_condition_cache = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    use_statistics_for_part_pruning = 0,
    -- parallel_replicas_plan_based = 0 - the plan-based path doubles the same analysis on its own (#118275).
    parallel_replicas_plan_based = 0,
    -- Prewhere is pinned on so the arms below are deterministic; the third one turns it off on purpose.
    optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- One index analysis per MergeTree read: the outer read and the IN subquery's read.
SELECT sum(key) FROM t_pr_in_set WHERE key IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1,
         log_comment = 'pr_local_plan_adopts_in_subquery_set_estimate';

-- Control: the same query with the row-count estimate off must give the same count.
SELECT sum(key) FROM t_pr_in_set WHERE key IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0,
         log_comment = 'pr_local_plan_adopts_in_subquery_set_no_estimate';

-- The same adoption path with prewhere off, a combination the runner draws on its own.
SELECT sum(key) FROM t_pr_in_set WHERE key IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1,
         optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
         log_comment = 'pr_local_plan_adopts_in_subquery_set_estimate_no_prewhere';

-- A GLOBAL IN still returns the right rows. Its subquery is materialized into a temporary table
-- before the local plan is built, so the re-planned set is keyed on that table and is never offered.
SELECT sum(key) FROM t_pr_in_set WHERE key GLOBAL IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1;

SET enable_parallel_replicas = 0;
SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['IndexAnalysisRounds'], ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment LIKE 'pr_local_plan_adopts_in_subquery_set_%'
ORDER BY log_comment;

DROP TABLE t_pr_in_set;
