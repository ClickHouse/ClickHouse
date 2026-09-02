SET max_threads = 1, max_insert_threads = 1, max_block_size = 128,
    max_insert_block_size = 128, min_insert_block_size_rows = 128,
    min_insert_block_size_bytes = 0, max_partitions_per_insert_block = 16;

CREATE TABLE dictionary_top_k
(
    part UInt8,
    id UInt64,
    k LowCardinality(String),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;

-- Each part has one distinct key, so a per-dictionary Top-K heap cannot skip or prune anything.
-- Separate partitions keep the dictionaries apart without stopping background merges.
INSERT INTO dictionary_top_k
SELECT toUInt8(intDiv(number, 8)), number, leftPad(toString(intDiv(number, 8)), 4, '0'), number % 8
FROM numbers(128);

SELECT throwIf(count() != 16 OR countIf(part_type = 'Wide' AND rows = 8) != 16,
    'Expected 16 Wide parts with one distinct key each')
FROM system.parts
WHERE database = currentDatabase() AND table = 'dictionary_top_k' AND active
FORMAT Null;

CREATE VIEW dictionary_top_k_input AS
SELECT k, value FROM dictionary_top_k ORDER BY id;

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
    enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 0,
    group_by_top_k_optimization_observation_rows = 0, exact_rows_before_limit = 0,
    use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0, use_query_cache = 0,
    log_queries = 1, log_profile_events = 1, log_queries_probability = 1,
    log_queries_min_query_duration_ms = 0, log_queries_min_type = 'QUERY_FINISH';

-- The ordered producer must keep a heap across dictionary switches. Ascending Top-K skips
-- later parts; descending Top-K evicts earlier groups and destroys their aggregate states.
SELECT k, count(), sum(value), uniqExact(value)
FROM dictionary_top_k_input
GROUP BY k ORDER BY k ASC LIMIT 1
SETTINGS log_comment = '05090_dictionary_top_k/ascending';

SELECT k, count(), sum(value), uniqExact(value)
FROM dictionary_top_k_input
GROUP BY k ORDER BY k DESC LIMIT 1
SETTINGS log_comment = '05090_dictionary_top_k/descending';

-- With Top-K disabled, the same fixture must still exercise dictionary sharding.
SELECT k, count(), sum(value), uniqExact(value)
FROM dictionary_top_k_input
GROUP BY k ORDER BY k ASC LIMIT 1
SETTINGS enable_group_by_top_k_optimization = 0, log_comment = '05090_dictionary_top_k/control';

SYSTEM FLUSH LOGS query_log;

-- Assert pruning directly, rather than using allocator-dependent memory thresholds.
SELECT
    substring(log_comment, length('05090_dictionary_top_k/') + 1) AS mode,
    ProfileEvents['SelectedParts'] = 16 AS all_parts,
    read_rows = 128 AS all_rows,
    ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'] > 0 AS sharded,
    ProfileEvents['AggregationTopKRowsSkipped'] > 0 AS skipped,
    ProfileEvents['AggregationTopKKeysPruned'] > 0 AS pruned
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05090_dictionary_top_k/ascending', '05090_dictionary_top_k/descending', '05090_dictionary_top_k/control')
    AND type = 'QueryFinish'
ORDER BY mode;

DROP VIEW dictionary_top_k_input;
DROP TABLE dictionary_top_k;
