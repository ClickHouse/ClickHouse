SET log_queries = 1;
SET log_profile_events = 1;

-- Every keyed input variant remains single-level. The outer sum consumes the inner aggregate so
-- the analyzer cannot prune uniqExact.
SELECT sum(u)
FROM
(
    SELECT number % 1000 AS k, uniqExact(number) AS u
    FROM numbers_mt(200000)
    GROUP BY k
    SETTINGS
        max_threads = 4,
        max_block_size = 8192,
        group_by_two_level_threshold = 0,
        group_by_two_level_threshold_bytes = 0,
        collect_hash_table_stats_during_aggregation = 0,
        enable_adaptive_aggregator = 0
)
SETTINGS log_comment = '05059_aggregation_merge_single_level';

-- Force every producer to cross the two-level threshold, then consume every bucket.
SELECT sum(c)
FROM
(
    SELECT number % 100000 AS k, count() AS c
    FROM numbers_mt(400000)
    GROUP BY k
    SETTINGS
        max_threads = 4,
        max_block_size = 8192,
        group_by_two_level_threshold = 1,
        group_by_two_level_threshold_bytes = 0,
        collect_hash_table_stats_during_aggregation = 0,
        enable_adaptive_aggregator = 0
)
SETTINGS log_comment = '05059_aggregation_merge_two_level';

-- A no-key uniqExact with several large partial states takes stock master's existing parallel
-- two-level merge wave.
SELECT uniqExact(number)
FROM numbers_mt(1000000)
SETTINGS
    max_threads = 4,
    max_block_size = 8192,
    log_comment = '05059_uniq_exact_merge_wave';

SYSTEM FLUSH LOGS query_log;

SELECT
    argMax(ProfileEvents['AggregationMergePrepAllSingleLevel'], event_time_microseconds) = 1,
    argMax(ProfileEvents['AggregationFinalMergePathSingleLevel'], event_time_microseconds) = 1,
    argMax(ProfileEvents['AggregationFinalMergePathTwoLevel'], event_time_microseconds) = 0,
    argMax(ProfileEvents['AggregationMergeInputVariants'], event_time_microseconds) > 1,
    argMax(ProfileEvents['AggregationMergeInputGroups'], event_time_microseconds) > 0,
    argMax(ProfileEvents['UniqExactMergeWaves'], event_time_microseconds) = 0
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_aggregation_merge_single_level';

SELECT
    argMax(ProfileEvents['AggregationMergePrepAllTwoLevel'], event_time_microseconds) = 1,
    argMax(ProfileEvents['AggregationFinalMergePathSingleLevel'], event_time_microseconds) = 0,
    argMax(ProfileEvents['AggregationFinalMergePathTwoLevel'], event_time_microseconds) = 1,
    argMax(ProfileEvents['AggregationMergeBuckets'], event_time_microseconds) = 256,
    argMax(ProfileEvents['AggregationMergeBucketElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationMergeBusiestBucketElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationMergeBusiestBucketElapsedMicroseconds'], event_time_microseconds)
        <= argMax(ProfileEvents['AggregationMergeBucketElapsedMicroseconds'], event_time_microseconds),
    argMax(ProfileEvents['AggregationMergeSources'], event_time_microseconds) BETWEEN 1 AND 4,
    argMax(ProfileEvents['AggregationMergeBusiestSourceElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationMergeBusiestSourceElapsedMicroseconds'], event_time_microseconds)
        <= argMax(ProfileEvents['AggregationMergeBucketElapsedMicroseconds'], event_time_microseconds)
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_aggregation_merge_two_level';

SELECT
    argMax(ProfileEvents['UniqExactMergeWaves'], event_time_microseconds) > 0,
    argMax(ProfileEvents['UniqExactMergeWaveInputStates'], event_time_microseconds) > 1,
    argMax(ProfileEvents['UniqExactMergeWaveElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['UniqExactMergeWaveCPUTimeMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['UniqExactMergeWaveWorkers'], event_time_microseconds) BETWEEN 1 AND 4
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_uniq_exact_merge_wave';
