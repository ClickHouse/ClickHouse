SET use_statistics_cache = 0, use_statistics_for_part_pruning = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET materialize_statistics_on_insert = 1, max_threads = 1;
SET optimize_move_to_prewhere = 1, enable_parallel_replicas = 0;

CREATE TABLE prewhere_planning_statistics (p UInt64, value UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0,
    min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = '5G';
INSERT INTO prewhere_planning_statistics SELECT intDiv(number, 1000), number % 1000 FROM numbers(8000);

-- The legacy AST optimizer runs before the query plan's partition analysis.
SET enable_analyzer = 0, query_plan_optimize_prewhere = 0;
SELECT sum(value) FROM prewhere_planning_statistics WHERE p = 3 AND value % 2 = 0
SETTINGS use_statistics = 0, log_comment = '05210_legacy_off';
SELECT sum(value) FROM prewhere_planning_statistics WHERE p = 3 AND value % 2 = 0
SETTINGS use_statistics = 1, log_comment = '05210_legacy_on';

-- An unbuilt set must not execute during statistics pruning. Its index usage
-- is checked by the execution plan after the set has been built.
SELECT sum(value) FROM prewhere_planning_statistics
WHERE p IN (SELECT number % 1 FROM numbers(1000)) AND value % 2 = 0
SETTINGS use_statistics = 1, force_index_by_date = 1, max_rows_to_read = 2001;

-- Automatic replica selection also optimizes a statistics-only plan without indexes.
SET enable_analyzer = 1, query_plan_optimize_prewhere = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2;
SET parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1, parallel_replicas_plan_based = 1;
SELECT sum(value) FROM prewhere_planning_statistics WHERE p = 3 AND value % 2 = 0
SETTINGS use_statistics = 0, log_comment = '05210_probe_off';
SELECT sum(value) FROM prewhere_planning_statistics WHERE p = 3 AND value % 2 = 0
SETTINGS use_statistics = 1, log_comment = '05210_probe_on';
SELECT sum(value) FROM prewhere_planning_statistics
WHERE p IN (SELECT number % 1 FROM numbers(1000)) AND value % 2 = 0
SETTINGS use_statistics = 1, force_index_by_date = 1, max_rows_to_read = 2001;

SET enable_parallel_replicas = 0;
SYSTEM FLUSH LOGS query_log;
SELECT
    maxIf(locks, log_comment = '05210_legacy_on') - maxIf(locks, log_comment = '05210_legacy_off') = 1,
    maxIf(locks, log_comment = '05210_probe_on') - maxIf(locks, log_comment = '05210_probe_off') = 2
FROM
(
    SELECT log_comment, toInt64(ProfileEvents['SharedPartsLocks']) AS locks
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '05210_')
);
DROP TABLE prewhere_planning_statistics;
