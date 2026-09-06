-- Tags: no-parallel
-- The failpoint is server-wide and can be consumed by other dictionary-aggregation queries.

SET max_threads = 1, max_insert_threads = 1, max_block_size = 1024,
    max_insert_block_size = 1024, min_insert_block_size_rows = 1024,
    min_insert_block_size_bytes = 0;

CREATE TABLE dictionary_normalization_split
(
    part UInt8,
    id UInt64,
    k LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;

INSERT INTO dictionary_normalization_split VALUES (0, 0, '0');
INSERT INTO dictionary_normalization_split SELECT 1, number + 1, toString(number + 1) FROM numbers(64);

SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide') != 2
    OR countIf(rows = 1) != 1 OR countIf(rows = 64) != 1,
    'Expected one small and one large Wide part')
FROM system.parts
WHERE database = currentDatabase() AND table = 'dictionary_normalization_split' AND active
FORMAT Null;

SET max_threads = 2, max_threads_min_free_memory_per_thread = 0,
    enable_parallel_replicas = 0, max_streams_for_merge_tree_reading = 1,
    max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0,
    preferred_block_size_bytes = 0, merge_tree_use_deserialization_prefixes_cache = 1,
    query_plan_lift_up_union = 0, optimize_read_in_order = 0,
    optimize_aggregation_in_order = 0, enable_adaptive_aggregator = 0,
    collect_hash_table_stats_during_aggregation = 0, compile_aggregate_expressions = 0,
    max_rows_to_group_by = 0, group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 0, max_bytes_before_external_group_by = 0,
    max_bytes_ratio_before_external_group_by = 0;

-- Separate input streams keep both variants alive until preparation. Their dictionaries
-- differ, so normalization is needed; only the large variant exceeds the splitting threshold.
-- The writer uses global `LowCardinality` settings, not session settings. Keep the dictionaries
-- within the default limit and use a failpoint to lower the splitting threshold to 32 keys.
-- `groupArraySorted` exercises destruction of non-trivial aggregate states.
CREATE VIEW dictionary_normalization_split_result AS
SELECT k, count() AS n, sum(id) AS s, groupArraySorted(2)(id) AS a
FROM
(
    SELECT k, id FROM dictionary_normalization_split WHERE part = 0
    UNION ALL
    SELECT k, id FROM dictionary_normalization_split WHERE part = 1
)
GROUP BY k;

-- Throw after transferring buckets to the new shards, while the caller still owns the old
-- vector. Both the split and unsplit source entries must remain valid during unwinding.
SYSTEM ENABLE FAILPOINT dictionary_aggregation_small_normalization_shards;
SYSTEM ENABLE FAILPOINT dictionary_aggregation_throw_after_normalization_split;
SELECT count(), sum(n), sum(s), sum(arraySum(a)) FROM dictionary_normalization_split_result; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT dictionary_aggregation_throw_after_normalization_split;

-- Keep the small-shard failpoint enabled to check successful splitting with the same fixture.
SELECT count(), sum(n), sum(s), sum(arraySum(a)) FROM dictionary_normalization_split_result;
SYSTEM DISABLE FAILPOINT dictionary_aggregation_small_normalization_shards;

DROP VIEW dictionary_normalization_split_result;
DROP TABLE dictionary_normalization_split;
