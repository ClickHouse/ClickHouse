-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- A scalar predicate on the partition key must restrict PREWHERE statistics to its parts.
SET enable_analyzer = 1, use_statistics_cache = 0, use_statistics_for_part_pruning = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET materialize_statistics_on_insert = 1, max_threads = 1;

CREATE TABLE prewhere_partition_statistics
(
    team_id UInt32,
    ts Date,
    value UInt64
)
ENGINE = MergeTree
PARTITION BY (team_id, toYYYYMM(ts))
ORDER BY (team_id, ts)
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0,
    min_bytes_for_full_part_storage = '5G';

-- One insert creates exactly one part per partition; no merges need to be stopped.
INSERT INTO prewhere_partition_statistics
SELECT intDiv(number, 2000), if(number % 2000 < 1000, toDate('2026-07-01'), toDate('2026-08-01')), number % 1000
FROM numbers(32000);

SELECT sum(value) FROM prewhere_partition_statistics
WHERE team_id = 3 AND ts = '2026-08-01' AND value % 2 = 0
SETTINGS use_statistics = 0, log_comment = '05182_selected_off';
SELECT sum(value) FROM prewhere_partition_statistics
WHERE team_id = 3 AND ts = '2026-08-01' AND value % 2 = 0
SETTINGS use_statistics = 1, log_comment = '05182_selected_on';

SELECT sum(value) FROM prewhere_partition_statistics WHERE value > 0 AND value < 1000
SETTINGS use_statistics = 0, log_comment = '05182_all_off';
SELECT sum(value) FROM prewhere_partition_statistics WHERE value > 0 AND value < 1000
SETTINGS use_statistics = 1, log_comment = '05182_all_on';

SELECT sum(value) FROM prewhere_partition_statistics
WHERE team_id = 99 AND ts = '2026-08-01' AND value % 2 = 0
SETTINGS use_statistics = 1, log_comment = '05182_absent';

-- Disabling partition pruning must still keep the complete statistics scope.
SELECT sum(value) FROM prewhere_partition_statistics
WHERE team_id = 3 AND ts = '2026-08-01' AND value % 2 = 0
SETTINGS use_statistics = 1, use_partition_pruning = 0, log_comment = '05182_pruning_off';
SELECT sum(value) FROM prewhere_partition_statistics
WHERE team_id = 3 AND ts = '2026-08-01' AND value % 2 = 0
SETTINGS use_statistics = 0, use_partition_pruning = 0, log_comment = '05182_both_off';

SYSTEM FLUSH LOGS query_log;

-- Each uncached part-statistics load takes one shared parts lock. Subtract the
-- identical query with statistics disabled to exclude locks for the actual read.
SELECT
    maxIf(locks, log_comment = '05182_selected_on') - maxIf(locks, log_comment = '05182_selected_off') = 1,
    maxIf(locks, log_comment = '05182_all_on') - maxIf(locks, log_comment = '05182_all_off') = 32,
    maxIf(locks, log_comment = '05182_pruning_off') - maxIf(locks, log_comment = '05182_both_off') = 32,
    countIf(log_comment = '05182_absent' AND ProfileEvents['LoadedStatisticsMicroseconds'] = 0) = 1
FROM
(
    SELECT log_comment, ProfileEvents, toInt64(ProfileEvents['SharedPartsLocks']) AS locks
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(log_comment, '05182_')
);

DROP TABLE prewhere_partition_statistics;
