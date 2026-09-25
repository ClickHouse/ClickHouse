-- Tags: no-random-settings

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
        max_threads_min_free_memory_per_thread = 0,
        max_block_size = 8192,
        group_by_two_level_threshold = 0,
        group_by_two_level_threshold_bytes = 0,
        max_bytes_before_external_group_by = 0,
        max_bytes_ratio_before_external_group_by = 0,
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
        max_threads_min_free_memory_per_thread = 0,
        max_block_size = 8192,
        group_by_two_level_threshold = 1,
        group_by_two_level_threshold_bytes = 0,
        max_bytes_before_external_group_by = 0,
        max_bytes_ratio_before_external_group_by = 0,
        collect_hash_table_stats_during_aggregation = 0,
        enable_adaptive_aggregator = 0
)
SETTINGS log_comment = '05059_aggregation_merge_two_level';

-- Keep one UNION ALL branch above the two-level threshold and the other below it.
SELECT sum(c)
FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT number AS k FROM numbers(100000)
        UNION ALL
        SELECT number % 1 AS k FROM numbers(100000)
    )
    GROUP BY k
    SETTINGS
        max_threads = 2,
        max_threads_min_free_memory_per_thread = 0,
        max_block_size = 8192,
        group_by_two_level_threshold = 1000,
        group_by_two_level_threshold_bytes = 0,
        max_bytes_before_external_group_by = 0,
        max_bytes_ratio_before_external_group_by = 0,
        collect_hash_table_stats_during_aggregation = 0,
        enable_adaptive_aggregator = 0
)
SETTINGS
    collect_hash_table_stats_during_aggregation = 0,
    log_comment = '05059_aggregation_merge_mixed_level';

-- Converting one producer's table to two-level is not an in-memory fan-in merge.
SELECT sum(c)
FROM
(
    SELECT number % 100000 AS k, count() AS c
    FROM numbers_mt(200000)
    GROUP BY k
    SETTINGS
        max_threads = 1,
        max_threads_min_free_memory_per_thread = 0,
        max_block_size = 8192,
        group_by_two_level_threshold = 1,
        group_by_two_level_threshold_bytes = 0,
        max_bytes_before_external_group_by = 1000000000,
        max_bytes_ratio_before_external_group_by = 0,
        collect_hash_table_stats_during_aggregation = 0,
        enable_adaptive_aggregator = 0
)
SETTINGS log_comment = '05059_aggregation_merge_single_variant';

-- arrayJoin emits a zero-row chunk that initializes a second producer without adding keyed
-- groups. It must not turn the one real two-level input into an observable fan-in merge.
SELECT sum(c)
FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT number AS k FROM numbers(100000)
        UNION ALL
        SELECT arrayJoin(emptyArrayUInt64()) AS k FROM numbers(1)
    )
    GROUP BY k
    SETTINGS
        max_threads = 2,
        max_threads_min_free_memory_per_thread = 0,
        max_block_size = 8192,
        group_by_two_level_threshold = 1,
        group_by_two_level_threshold_bytes = 0,
        max_bytes_before_external_group_by = 0,
        max_bytes_ratio_before_external_group_by = 0,
        collect_hash_table_stats_during_aggregation = 0,
        enable_adaptive_aggregator = 0
)
SETTINGS
    collect_hash_table_stats_during_aggregation = 0,
    log_comment = '05059_aggregation_merge_zero_row_variant';

-- A no-key uniqExact with several large partial states takes stock master's existing parallel
-- two-level merge wave.
SELECT uniqExact(number)
FROM numbers_mt(1000000)
SETTINGS
    max_threads = 4,
    max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8192,
    log_comment = '05059_uniq_exact_merge_wave';

SYSTEM FLUSH LOGS query_log;

SELECT
    argMax(ProfileEvents['AggregationInMemoryMergeInputVariants'], event_time_microseconds) > 1,
    argMax(ProfileEvents['AggregationInMemoryMergeInputTwoLevelVariants'], event_time_microseconds) = 0,
    argMax(ProfileEvents['AggregationInMemoryMergeInputGroups'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationInMemoryMergePathSingleLevel'], event_time_microseconds) = 1,
    argMax(ProfileEvents['AggregationInMemoryMergePathTwoLevel'], event_time_microseconds) = 0,
    argMax(ProfileEvents['UniqExactMergeWaves'], event_time_microseconds) = 0
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_aggregation_merge_single_level';

SELECT
    argMax(ProfileEvents['AggregationInMemoryMergeInputVariants'], event_time_microseconds) > 1,
    argMax(ProfileEvents['AggregationInMemoryMergeInputTwoLevelVariants'], event_time_microseconds)
        = argMax(ProfileEvents['AggregationInMemoryMergeInputVariants'], event_time_microseconds),
    argMax(ProfileEvents['AggregationInMemoryMergePathSingleLevel'], event_time_microseconds) = 0,
    argMax(ProfileEvents['AggregationInMemoryMergePathTwoLevel'], event_time_microseconds) = 1,
    argMax(ProfileEvents['AggregationInMemoryMergeBuckets'], event_time_microseconds) = 256,
    argMax(ProfileEvents['AggregationInMemoryMergeBucketElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationInMemoryMergeBusiestBucketElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationInMemoryMergeBusiestBucketElapsedMicroseconds'], event_time_microseconds)
        <= argMax(ProfileEvents['AggregationInMemoryMergeBucketElapsedMicroseconds'], event_time_microseconds),
    argMax(ProfileEvents['AggregationInMemoryMergeSources'], event_time_microseconds) BETWEEN 1 AND 4,
    argMax(ProfileEvents['AggregationInMemoryMergeBusiestSourceElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationInMemoryMergeBusiestSourceElapsedMicroseconds'], event_time_microseconds)
        <= argMax(ProfileEvents['AggregationInMemoryMergeBucketElapsedMicroseconds'], event_time_microseconds)
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_aggregation_merge_two_level';

SELECT
    argMax(ProfileEvents['AggregationInMemoryMergeInputVariants'], event_time_microseconds) > 1,
    argMax(ProfileEvents['AggregationInMemoryMergeInputTwoLevelVariants'], event_time_microseconds) > 0,
    argMax(ProfileEvents['AggregationInMemoryMergeInputTwoLevelVariants'], event_time_microseconds)
        < argMax(ProfileEvents['AggregationInMemoryMergeInputVariants'], event_time_microseconds),
    argMax(ProfileEvents['AggregationInMemoryMergePathTwoLevel'], event_time_microseconds) = 1
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_aggregation_merge_mixed_level';

SELECT
    argMax(
        ProfileEvents['AggregationInMemoryMergeInputVariants']
            + ProfileEvents['AggregationInMemoryMergeInputTwoLevelVariants']
            + ProfileEvents['AggregationInMemoryMergeInputGroups']
            + ProfileEvents['AggregationInMemoryMergePathSingleLevel']
            + ProfileEvents['AggregationInMemoryMergePathTwoLevel']
            + ProfileEvents['AggregationInMemoryMergeBuckets']
            + ProfileEvents['AggregationInMemoryMergeBucketElapsedMicroseconds']
            + ProfileEvents['AggregationInMemoryMergeBusiestBucketElapsedMicroseconds']
            + ProfileEvents['AggregationInMemoryMergeSources']
            + ProfileEvents['AggregationInMemoryMergeBusiestSourceElapsedMicroseconds'],
        event_time_microseconds)
        = 0
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_aggregation_merge_single_variant';

SELECT
    argMax(
        ProfileEvents['AggregationInMemoryMergeInputVariants']
            + ProfileEvents['AggregationInMemoryMergeInputTwoLevelVariants']
            + ProfileEvents['AggregationInMemoryMergeInputGroups']
            + ProfileEvents['AggregationInMemoryMergePathSingleLevel']
            + ProfileEvents['AggregationInMemoryMergePathTwoLevel']
            + ProfileEvents['AggregationInMemoryMergeBuckets']
            + ProfileEvents['AggregationInMemoryMergeBucketElapsedMicroseconds']
            + ProfileEvents['AggregationInMemoryMergeBusiestBucketElapsedMicroseconds']
            + ProfileEvents['AggregationInMemoryMergeSources']
            + ProfileEvents['AggregationInMemoryMergeBusiestSourceElapsedMicroseconds'],
        event_time_microseconds)
        = 0
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_aggregation_merge_zero_row_variant';

SELECT
    argMax(ProfileEvents['UniqExactMergeWaves'], event_time_microseconds) > 0,
    argMax(ProfileEvents['UniqExactMergeWaveInputStates'], event_time_microseconds) > 1,
    argMax(ProfileEvents['UniqExactMergeWaveElapsedMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['UniqExactMergeWaveCPUTimeMicroseconds'], event_time_microseconds) > 0,
    argMax(ProfileEvents['UniqExactMergeWaveWorkers'], event_time_microseconds)
        BETWEEN argMax(ProfileEvents['UniqExactMergeWaves'], event_time_microseconds)
        AND 4 * argMax(ProfileEvents['UniqExactMergeWaves'], event_time_microseconds)
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '05059_uniq_exact_merge_wave';
