SET enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
-- Lock deltas measure one local planner, without additional parallel-replica candidate plans.
SET enable_parallel_replicas = 0;
SET use_statistics_cache = 0, use_query_cache = 0, use_query_condition_cache = 0;
SET materialize_statistics_on_insert = 1, max_threads = 1;

CREATE TABLE prewhere_statistics_pruned_parts (p UInt32, value UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0;

INSERT INTO prewhere_statistics_pruned_parts
SELECT intDiv(number, 10), intDiv(number, 10) * 100 + number % 10 FROM numbers(40);

-- Only statistics can exclude the first three parts: there is no filter on the partition key.
SELECT sum(value) FROM prewhere_statistics_pruned_parts WHERE value >= 300 AND value % 2 = 0
SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 1, log_comment = '05183_pruned_off';
SELECT sum(value) FROM prewhere_statistics_pruned_parts WHERE value >= 300 AND value % 2 = 0
SETTINGS use_statistics = 1, use_statistics_for_part_pruning = 1, log_comment = '05183_pruned_on';

SELECT sum(value) FROM prewhere_statistics_pruned_parts WHERE value >= 300 AND value % 2 = 0
SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 0, log_comment = '05183_all_off';
SELECT sum(value) FROM prewhere_statistics_pruned_parts WHERE value >= 300 AND value % 2 = 0
SETTINGS use_statistics = 1, use_statistics_for_part_pruning = 0, log_comment = '05183_all_on';

SYSTEM FLUSH LOGS query_log;

-- An uncached statistics-estimator load takes one shared parts lock per part.
SELECT
    maxIf(locks, log_comment = '05183_pruned_on') - maxIf(locks, log_comment = '05183_pruned_off') = 1,
    maxIf(locks, log_comment = '05183_all_on') - maxIf(locks, log_comment = '05183_all_off') = 4
FROM
(
    SELECT log_comment, toInt64(ProfileEvents['SharedPartsLocks']) AS locks
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(log_comment, '05183_')
);

DROP TABLE prewhere_statistics_pruned_parts;
