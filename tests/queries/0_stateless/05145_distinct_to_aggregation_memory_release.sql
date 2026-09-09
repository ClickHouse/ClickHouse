SET query_plan_convert_distinct_to_aggregation = 1;
SET distinct_overflow_mode = 'throw';
SET max_rows_in_distinct = 0;
SET max_threads = 2;
SET max_block_size = 100000;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET enable_parallel_single_level_merge = 0;
SET enable_adaptive_aggregator = 0;

-- Source tables and completed buckets release their buffers while the remaining buckets merge.
SELECT count(), sum(k) FROM
(
    SELECT number AS k FROM numbers(100000)
    UNION DISTINCT SELECT number + 100000 AS k FROM numbers(100000)
)
SETTINGS max_bytes_in_distinct = 5242880;

-- Nullable key tables release their bucket buffers under the same byte bound.
SELECT count(), sum(k) FROM
(
    SELECT toNullable(number) AS k FROM numbers(100000)
    UNION DISTINCT SELECT toNullable(number + 100000) AS k FROM numbers(100000)
)
SETTINGS max_bytes_in_distinct = 5242880;

-- Staged keys stay charged until their last bucket retires, while completed bucket tables are freed.
SELECT count(), sum(k) FROM
(
    SELECT number AS k FROM numbers(100000)
    UNION DISTINCT SELECT number + 100000 AS k FROM numbers(100000)
)
SETTINGS max_bytes_in_distinct = 6000000, enable_adaptive_aggregator = 1,
    adaptive_aggregator_freeze_threshold = 0, adaptive_aggregator_freeze_threshold_bytes = 0;

-- Single-level merging releases each source table before merging the next one.
SELECT count(), sum(k) FROM
(
    SELECT number + 0 AS k FROM numbers(10000)
    UNION DISTINCT SELECT number + 10000 AS k FROM numbers(10000)
    UNION DISTINCT SELECT number + 20000 AS k FROM numbers(10000)
    UNION DISTINCT SELECT number + 30000 AS k FROM numbers(10000)
    UNION DISTINCT SELECT number + 40000 AS k FROM numbers(10000)
    UNION DISTINCT SELECT number + 50000 AS k FROM numbers(10000)
    UNION DISTINCT SELECT number + 60000 AS k FROM numbers(10000)
    UNION DISTINCT SELECT number + 70000 AS k FROM numbers(10000)
)
SETTINGS max_threads = 8, max_bytes_in_distinct = 5242880, group_by_two_level_threshold = 0;
