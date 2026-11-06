-- Tags: no-replicated-database
-- - no-replicated-database - the test reads through a two-replica cluster, while a Replicated
--   database test uses a different cluster

DROP VIEW IF EXISTS v_pr_in_set_memory;
DROP TABLE IF EXISTS t_pr_in_set_memory;
DROP TABLE IF EXISTS t_pr_in_set;
CREATE TABLE t_pr_in_set (key Int, value Int) ENGINE = MergeTree() ORDER BY key;
SYSTEM STOP MERGES t_pr_in_set;
INSERT INTO t_pr_in_set SELECT number, number * 100 FROM numbers(100000)
SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;
CREATE TABLE t_pr_in_set_memory (key Int) ENGINE = Memory;
INSERT INTO t_pr_in_set_memory SELECT number FROM numbers(1000);
CREATE VIEW v_pr_in_set_memory AS SELECT key FROM t_pr_in_set_memory;

SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    automatic_parallel_replicas_mode = 0, use_query_condition_cache = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    use_statistics_for_part_pruning = 0,
    -- parallel_replicas_plan_based = 0 - the plan-based path doubles the same analysis on its own (#118275).
    parallel_replicas_plan_based = 0,
    -- Prewhere is pinned on so the arms below are deterministic; one arm turns it off on purpose.
    optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- One index analysis per MergeTree read: the outer read and the IN subquery's read.
SELECT sum(key) FROM t_pr_in_set WHERE key IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1, max_rows_to_read = 0,
         log_comment = 'pr_in_subquery_read_analyzed_once_estimate';

-- A read row limit makes the row estimate analyze apart from the read it plans.
SELECT sum(key) FROM t_pr_in_set WHERE key IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1, max_rows_to_read = 20000000,
         log_comment = 'pr_in_subquery_read_analyzed_once_estimate_row_limit';

-- The same with prewhere off, a combination the runner draws on its own.
SELECT sum(key) FROM t_pr_in_set WHERE key IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1,
         optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
         log_comment = 'pr_in_subquery_read_analyzed_once_estimate_no_prewhere';

-- A GLOBAL IN subquery is planned for the collected filters the same way.
SELECT sum(key) FROM t_pr_in_set WHERE key GLOBAL IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1, max_rows_to_read = 0,
         log_comment = 'pr_in_subquery_read_analyzed_once_global_in';

-- A view collects filters too; the materialized CTE's read keeps parallel replicas.
WITH c AS MATERIALIZED (SELECT key FROM t_pr_in_set)
SELECT sum(key) FROM v_pr_in_set_memory WHERE key IN (SELECT key FROM c UNION ALL SELECT key FROM c)
SETTINGS enable_materialized_cte = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
         log_comment = 'pr_in_subquery_read_analyzed_once_materialized_cte';

-- Control: the same query with the row estimate off.
SELECT sum(key) FROM t_pr_in_set WHERE key IN (SELECT key FROM t_pr_in_set WHERE key > 50000)
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0,
         log_comment = 'pr_in_subquery_read_analyzed_once_no_estimate';

SET enable_parallel_replicas = 0;
SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['IndexAnalysisRounds'], ProfileEvents['ParallelReplicasUsedCount'] > 0, ProfileEvents['ParallelReplicasQueryCount']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment LIKE 'pr_in_subquery_read_analyzed_once_%'
ORDER BY log_comment;

DROP VIEW v_pr_in_set_memory;
DROP TABLE t_pr_in_set_memory;
DROP TABLE t_pr_in_set;
