DROP TABLE IF EXISTS low_cardinality_dictionary_index_premerge_05046;

-- Keep dictionary pre-merging eligible regardless of the server profile or randomized test settings.
SET
    max_rows_to_group_by = 0,
    optimize_aggregation_in_order = 0,
    enable_adaptive_aggregator = 0,
    collect_hash_table_stats_during_aggregation = 0,
    max_bytes_before_external_group_by = 0,
    max_bytes_ratio_before_external_group_by = 0;

CREATE TABLE low_cardinality_dictionary_index_premerge_05046
(
    key LowCardinality(String),
    value UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    index_granularity = 1024,
    index_granularity_bytes = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES low_cardinality_dictionary_index_premerge_05046;

INSERT INTO low_cardinality_dictionary_index_premerge_05046
SELECT toString(number % 100), number
FROM numbers(10000)
SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 10000;

INSERT INTO low_cardinality_dictionary_index_premerge_05046
SELECT toString(number % 100), number + 10000
FROM numbers(10000)
SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 10000;

SELECT throwIf(
    count() != 2 OR countIf(part_type = 'Wide') != 2 OR countIf(rows = 10000) != 2,
    'Expected two Wide parts with 10000 rows each for dictionary pre-merging')
FROM system.parts
WHERE database = currentDatabase()
    AND table = 'low_cardinality_dictionary_index_premerge_05046'
    AND active
FORMAT Null;

SELECT count(), sum(group_count), sum(value_sum)
FROM
(
    SELECT key, count() AS group_count, sum(value) AS value_sum
    FROM low_cardinality_dictionary_index_premerge_05046
    GROUP BY key
)
SETTINGS
    max_threads = 8,
    group_by_two_level_threshold = 0,
    group_by_two_level_threshold_bytes = 0,
    merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 0,
    merge_tree_min_read_task_size = 1,
    merge_tree_min_bytes_per_read_stream = 0;

SELECT count(), sum(group_count), sum(value_sum)
FROM
(
    SELECT key, count() AS group_count, sum(value) AS value_sum
    FROM low_cardinality_dictionary_index_premerge_05046
    GROUP BY key
)
SETTINGS
    max_threads = 8,
    group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 0,
    merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 0,
    merge_tree_min_read_task_size = 1,
    merge_tree_min_bytes_per_read_stream = 0;

SELECT count(), sum(group_count)
FROM
(
    SELECT key, count() AS group_count
    FROM low_cardinality_dictionary_index_premerge_05046
    GROUP BY key
)
SETTINGS
    max_threads = 8,
    group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 0,
    merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 0,
    merge_tree_min_read_task_size = 1,
    merge_tree_min_bytes_per_read_stream = 0;

SELECT count(), sum(length(group_values)), sum(arraySum(group_values))
FROM
(
    SELECT key, groupArray(value) AS group_values
    FROM low_cardinality_dictionary_index_premerge_05046
    GROUP BY key
)
SETTINGS
    max_threads = 8,
    group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 0,
    merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 0,
    merge_tree_min_read_task_size = 1,
    merge_tree_min_bytes_per_read_stream = 0;

DROP TABLE low_cardinality_dictionary_index_premerge_05046;
