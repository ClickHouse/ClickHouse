SET max_threads = 1, max_insert_threads = 1;

CREATE TABLE dictionary_group_array_insert_at
(
    part UInt8,
    id UInt64,
    k LowCardinality(String),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;

-- Separate partitions preserve the two dictionaries without stopping background merges.
INSERT INTO dictionary_group_array_insert_at VALUES (0, 0, 'a', 10);
INSERT INTO dictionary_group_array_insert_at VALUES (1, 1, 'a', 20), (1, 2, 'b', 30);

SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide') != 2
    OR countIf(rows = 1) != 1 OR countIf(rows = 2) != 1,
    'Expected one one-row and one two-row Wide part')
FROM system.parts
WHERE database = currentDatabase() AND table = 'dictionary_group_array_insert_at' AND active
FORMAT Null;

CREATE VIEW dictionary_group_array_insert_at_input AS
SELECT k, value FROM dictionary_group_array_insert_at ORDER BY id;

SET max_threads = 1, enable_parallel_replicas = 0, serialize_query_plan = 0,
    max_streams_for_merge_tree_reading = 1, max_block_size = 1, preferred_block_size_bytes = 0,
    merge_tree_use_deserialization_prefixes_cache = 1,
    optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0,
    optimize_aggregation_in_order = 0, enable_adaptive_aggregator = 0,
    allow_aggregate_partitions_independently = 0, force_aggregate_partitions_independently = 0,
    enable_lazy_columns_replication = 0, collect_hash_table_stats_during_aggregation = 0,
    compile_aggregate_expressions = 0, max_rows_to_group_by = 0,
    group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    use_query_cache = 0, log_queries = 1, log_profile_events = 1, log_queries_probability = 1,
    log_queries_min_query_duration_ms = 0, log_queries_min_type = 'QUERY_FINISH';

SELECT 'String control', CAST(k AS String) AS key, groupArrayInsertAt(value, 0)
FROM dictionary_group_array_insert_at_input
GROUP BY key ORDER BY key;

-- The first value of `a` must win. Sharding the later dictionary would create a larger
-- table, which the final merge would select as its destination and incorrectly keep 20.
SELECT 'single-level', k, groupArrayInsertAt(value, 0)
FROM dictionary_group_array_insert_at_input
GROUP BY k ORDER BY k
SETTINGS log_comment = '05091_dictionary_group_array_insert_at/single-level';

SELECT 'two-level', k, groupArrayInsertAt(value, 0)
FROM dictionary_group_array_insert_at_input
GROUP BY k ORDER BY k
SETTINGS group_by_two_level_threshold = 1, log_comment = '05091_dictionary_group_array_insert_at/two-level';

-- Verify that the same ordered input still allows sharding for an order-independent aggregate.
SELECT 'sum control', k, sum(value)
FROM dictionary_group_array_insert_at_input
GROUP BY k ORDER BY k
SETTINGS log_comment = '05091_dictionary_group_array_insert_at/control';

SYSTEM FLUSH LOGS query_log;

SELECT
    substring(log_comment, length('05091_dictionary_group_array_insert_at/') + 1) AS mode,
    ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'] > 0 AS sharded
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN (
        '05091_dictionary_group_array_insert_at/control',
        '05091_dictionary_group_array_insert_at/single-level',
        '05091_dictionary_group_array_insert_at/two-level')
    AND type = 'QueryFinish'
ORDER BY mode;

DROP VIEW dictionary_group_array_insert_at_input;
DROP TABLE dictionary_group_array_insert_at;
