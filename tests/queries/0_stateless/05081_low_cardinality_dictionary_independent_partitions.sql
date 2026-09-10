-- Each partition has several dictionaries but only one grouping key. Independent
-- partition aggregation must emit one complete result for that key, not a partial
-- result for each dictionary or for the producer's retired-result table.
SET
    max_threads = 2,
    max_insert_threads = 1,
    enable_parallel_replicas = 0,
    explain_query_plan_default = 'legacy',
    max_block_size = 1,
    preferred_block_size_bytes = 0,
    low_cardinality_use_single_dictionary_for_part = 1,
    merge_tree_use_deserialization_prefixes_cache = 1,
    optimize_read_in_order = 0,
    optimize_aggregation_in_order = 0,
    enable_adaptive_aggregator = 0,
    collect_hash_table_stats_during_aggregation = 0,
    compile_aggregate_expressions = 0,
    max_rows_to_group_by = 0,
    group_by_two_level_threshold_bytes = 0,
    max_bytes_before_external_group_by = 1073741824,
    max_bytes_ratio_before_external_group_by = 0,
    allow_aggregate_partitions_independently = 0,
    force_aggregate_partitions_independently = 0;

DROP TABLE IF EXISTS low_cardinality_dictionary_independent_partitions;
CREATE TABLE low_cardinality_dictionary_independent_partitions
(
    k LowCardinality(String),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY k
ORDER BY tuple()
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES low_cardinality_dictionary_independent_partitions;

INSERT INTO low_cardinality_dictionary_independent_partitions VALUES ('left', 10), ('right', 100);
INSERT INTO low_cardinality_dictionary_independent_partitions VALUES ('left', 20), ('right', 200);
INSERT INTO low_cardinality_dictionary_independent_partitions VALUES ('left', 30), ('right', 300);

SELECT throwIf(count() != 2 OR countIf(part_count = 3 AND wide_parts = 3) != 2,
    'Expected two partitions with three single-row Wide parts each')
FROM
(
    SELECT partition_id, count() AS part_count, countIf(part_type = 'Wide' AND rows = 1) AS wide_parts
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 'low_cardinality_dictionary_independent_partitions'
        AND active
    GROUP BY partition_id
)
FORMAT Null;

-- A high nonzero spill threshold permits two-level aggregation even if the ordinary
-- read is reduced to one stream. It must not spill this tiny query: the external merge
-- could otherwise mask the missing in-memory merge between dictionary variants.
SET group_by_two_level_threshold = 0;

SELECT 'ordinary aggregation, two-level threshold 0';
SELECT k, count() AS n, sum(value) AS total, groupArraySorted(3)(value) AS values
FROM low_cardinality_dictionary_independent_partitions
GROUP BY k
ORDER BY k, n, total, values;

SET allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1;

SELECT 'independent partitions, two-level threshold 0';
SELECT
    'partition plan',
    countIf(explain LIKE '%Skip merging: 1%') = 1,
    countIf(explain LIKE '%Read each partition through separate port: 1%') = 1
FROM
(
    EXPLAIN actions = 1
    SELECT k, count() AS n, sum(value) AS total, groupArraySorted(3)(value) AS values
    FROM low_cardinality_dictionary_independent_partitions
    GROUP BY k
    ORDER BY k, n, total, values
);
SELECT k, count() AS n, sum(value) AS total, groupArraySorted(3)(value) AS values
FROM low_cardinality_dictionary_independent_partitions
GROUP BY k
ORDER BY k, n, total, values;

SET group_by_two_level_threshold = 1,
    allow_aggregate_partitions_independently = 0,
    force_aggregate_partitions_independently = 0;

SELECT 'ordinary aggregation, two-level threshold 1';
SELECT k, count() AS n, sum(value) AS total, groupArraySorted(3)(value) AS values
FROM low_cardinality_dictionary_independent_partitions
GROUP BY k
ORDER BY k, n, total, values;

SET allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1;

SELECT 'independent partitions, two-level threshold 1';
SELECT
    'partition plan',
    countIf(explain LIKE '%Skip merging: 1%') = 1,
    countIf(explain LIKE '%Read each partition through separate port: 1%') = 1
FROM
(
    EXPLAIN actions = 1
    SELECT k, count() AS n, sum(value) AS total, groupArraySorted(3)(value) AS values
    FROM low_cardinality_dictionary_independent_partitions
    GROUP BY k
    ORDER BY k, n, total, values
);
SELECT k, count() AS n, sum(value) AS total, groupArraySorted(3)(value) AS values
FROM low_cardinality_dictionary_independent_partitions
GROUP BY k
ORDER BY k, n, total, values;

DROP TABLE low_cardinality_dictionary_independent_partitions;
