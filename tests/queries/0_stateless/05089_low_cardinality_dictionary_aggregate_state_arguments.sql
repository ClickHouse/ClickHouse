-- Tags: no-parallel
-- The drain failpoint is server-wide and can be consumed by other dictionary-shard queries.

SET max_threads = 1, max_insert_threads = 1, max_block_size = 64,
    max_insert_block_size = 64, min_insert_block_size_rows = 64,
    min_insert_block_size_bytes = 0;

CREATE TABLE dictionary_state_arguments
(
    phase UInt8,
    id UInt64,
    k LowCardinality(String),
    s AggregateFunction(uniqExact, UInt64)
)
ENGINE = MergeTree
PARTITION BY phase
ORDER BY id
SETTINGS index_granularity = 32, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;

INSERT INTO dictionary_state_arguments
SELECT toUInt8(intDiv(number, 32)), number,
    toString(if(number < 32, number, 63 - number)), uniqExactState(number)
FROM numbers(64)
GROUP BY number
ORDER BY number;

SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide' AND rows = 32) != 2,
    'Expected two Wide parts with different key dictionaries')
FROM system.parts
WHERE database = currentDatabase() AND table = 'dictionary_state_arguments' AND active
FORMAT Null;

-- Each ordered input crosses a part boundary, enabling sharding without the eligibility check.
CREATE VIEW dictionary_state_input AS
SELECT k, id, s FROM dictionary_state_arguments ORDER BY id;

SET max_threads = 2, max_threads_min_free_memory_per_thread = 0,
    enable_parallel_replicas = 0, max_streams_for_merge_tree_reading = 1,
    max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0,
    max_block_size = 1, preferred_block_size_bytes = 0,
    merge_tree_use_deserialization_prefixes_cache = 1,
    optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0,
    query_plan_lift_up_union = 0, query_plan_remove_unused_columns = 1,
    optimize_aggregation_in_order = 0,
    enable_adaptive_aggregator = 0, enable_lazy_columns_replication = 0,
    collect_hash_table_stats_during_aggregation = 0, compile_aggregate_expressions = 0,
    max_rows_to_group_by = 0, group_by_two_level_threshold = 0,
    group_by_two_level_threshold_bytes = 0, max_bytes_before_external_group_by = 0,
    max_bytes_ratio_before_external_group_by = 0;

-- State arguments must stay producer-local even when dictionaries change. Check the results
-- with the shard-drain exception armed, rather than retaining enough states to exhaust memory.
SYSTEM ENABLE FAILPOINT dictionary_aggregation_throw_before_drain;
SELECT count(), min(n), max(n), sum(n)
FROM
(
    SELECT k, uniqExactMerge(s) AS n
    FROM (SELECT * FROM dictionary_state_input UNION ALL SELECT * FROM dictionary_state_input)
    GROUP BY k
);

-- The guard must also recognize states nested inside argument columns.
SELECT count(), min(n), max(n), sum(n)
FROM
(
    SELECT k, uniqExactMergeArray([s]) AS n
    FROM (SELECT * FROM dictionary_state_input UNION ALL SELECT * FROM dictionary_state_input)
    GROUP BY k
);

SELECT count(), min(n), max(n), sum(n)
FROM
(
    SELECT k, uniqExactMergeArrayArray([[s]]) AS n
    FROM (SELECT * FROM dictionary_state_input UNION ALL SELECT * FROM dictionary_state_input)
    GROUP BY k
);

-- Scalar arguments must still use shards, even if the aggregate returns states. This also proves
-- that the fixture reaches the guarded path and the successful queries did not consume the failpoint.
SELECT k, uniqExactState(id)
FROM (SELECT k, id FROM dictionary_state_input UNION ALL SELECT k, id FROM dictionary_state_input)
GROUP BY k
FORMAT Null; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT dictionary_aggregation_throw_before_drain;

DROP VIEW dictionary_state_input;
DROP TABLE dictionary_state_arguments;
