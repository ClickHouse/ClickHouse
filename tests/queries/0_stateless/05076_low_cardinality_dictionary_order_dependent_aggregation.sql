DROP TABLE IF EXISTS low_cardinality_dictionary_order_dependent_aggregation;

CREATE TABLE low_cardinality_dictionary_order_dependent_aggregation
(
    id UInt64,
    key LowCardinality(String),
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    index_granularity_bytes = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES low_cardinality_dictionary_order_dependent_aggregation;

INSERT INTO low_cardinality_dictionary_order_dependent_aggregation
VALUES (0, 'shared', 10), (2, 'shared', 0);

INSERT INTO low_cardinality_dictionary_order_dependent_aggregation
VALUES (1, 'shared', 5);

SELECT throwIf(
    count() != 2 OR countIf(part_type = 'Wide') != 2 OR sum(rows) != 3,
    'Expected two Wide parts with three rows for interleaved dictionary reads')
FROM system.parts
WHERE database = currentDatabase()
    AND table = 'low_cardinality_dictionary_order_dependent_aggregation'
    AND active
FORMAT Null;

SET
    max_threads = 1,
    max_block_size = 1,
    preferred_block_size_bytes = 0,
    optimize_read_in_order = 1,
    query_plan_remove_redundant_sorting = 0,
    optimize_aggregation_in_order = 0,
    enable_adaptive_aggregator = 0,
    max_rows_to_group_by = 0,
    max_bytes_before_external_group_by = 0,
    group_by_two_level_threshold_bytes = 0,
    group_by_two_level_threshold = 0;

SELECT 'String control, two-level threshold 0', group_key, deltaSum(value), groupArray(value)
FROM
(
    SELECT CAST(key AS String) AS group_key, value
    FROM low_cardinality_dictionary_order_dependent_aggregation
    ORDER BY id
)
GROUP BY group_key
ORDER BY group_key;

SELECT 'deltaSum, two-level threshold 0', key, deltaSum(value)
FROM
(
    SELECT key, value
    FROM low_cardinality_dictionary_order_dependent_aggregation
    ORDER BY id
)
GROUP BY key
ORDER BY key;

SELECT 'groupArray, two-level threshold 0', key, groupArray(value)
FROM
(
    SELECT key, value
    FROM low_cardinality_dictionary_order_dependent_aggregation
    ORDER BY id
)
GROUP BY key
ORDER BY key;

SET group_by_two_level_threshold = 1;

SELECT 'String control, two-level threshold 1', group_key, deltaSum(value), groupArray(value)
FROM
(
    SELECT CAST(key AS String) AS group_key, value
    FROM low_cardinality_dictionary_order_dependent_aggregation
    ORDER BY id
)
GROUP BY group_key
ORDER BY group_key;

SELECT 'deltaSum, two-level threshold 1', key, deltaSum(value)
FROM
(
    SELECT key, value
    FROM low_cardinality_dictionary_order_dependent_aggregation
    ORDER BY id
)
GROUP BY key
ORDER BY key;

SELECT 'groupArray, two-level threshold 1', key, groupArray(value)
FROM
(
    SELECT key, value
    FROM low_cardinality_dictionary_order_dependent_aggregation
    ORDER BY id
)
GROUP BY key
ORDER BY key;

DROP TABLE low_cardinality_dictionary_order_dependent_aggregation;
