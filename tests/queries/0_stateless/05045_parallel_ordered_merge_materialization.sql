-- Tags: no-random-merge-tree-settings, no-object-storage

DROP TABLE IF EXISTS parallel_ordered_merge_materialization;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE parallel_ordered_merge_materialization
(
    key UInt64,
    source UInt8,
    ids Array(LowCardinality(UInt128)),
    payload String,
    nullable_payload Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS index_granularity = 16;

SYSTEM STOP MERGES parallel_ordered_merge_materialization;

INSERT INTO parallel_ordered_merge_materialization
SELECT
    number,
    0,
    arrayMap(element -> toUInt128(number * 16 + element), range(4)),
    concat('source-0-key-', toString(number)),
    if(number % 5 = 0, NULL, number)
FROM numbers(256);

INSERT INTO parallel_ordered_merge_materialization
SELECT
    number,
    1,
    arrayMap(element -> bitShiftLeft(toUInt128(1), 60) + number * 16 + element, range(4)),
    concat('source-1-key-', toString(number)),
    if(number % 5 = 1, NULL, number + 1)
FROM numbers(256);

INSERT INTO parallel_ordered_merge_materialization
SELECT
    number + 1000,
    2,
    arrayMap(element -> bitShiftLeft(toUInt128(2), 60) + number * 16 + element, range(4)),
    concat('source-2-key-', toString(number)),
    if(number % 5 = 2, NULL, number + 2)
FROM numbers(256);

INSERT INTO parallel_ordered_merge_materialization
SELECT
    number + 2000,
    3,
    arrayMap(element -> bitShiftLeft(toUInt128(3), 60) + number * 16 + element, range(4)),
    concat('source-3-key-', toString(number)),
    if(number % 5 = 3, NULL, number + 3)
FROM numbers(256);

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 0;
SET read_in_order_use_buffering = 0;
SET read_in_order_two_level_merge_threshold = 0;
SET merge_tree_min_rows_for_concurrent_read = 1;
SET max_threads = 4;
SET max_streams_for_merge_tree_reading = 4;
SET max_block_size = 17;
SET max_parallel_ordered_merge_materialization_threads = 4;

SELECT
    countIf(explain LIKE '%MaterializeMergedDataTransform%') > 0,
    countIf(explain LIKE '%SortChunksBySequenceNumber%') > 0
FROM
(
    EXPLAIN PIPELINE compact = 1
    SELECT *
    FROM parallel_ordered_merge_materialization
    ORDER BY key
);

-- With two read streams and four parts, the reader builds two preliminary ordered merges
-- before the final `SortingStep` merge. All three merges must use parallel materialization.
SET max_threads = 2;
SET max_streams_for_merge_tree_reading = 2;
SET max_parallel_ordered_merge_materialization_threads = 2;

SELECT
    countIf(explain LIKE '%MaterializeMergedDataTransform%') = 3,
    countIf(explain LIKE '%SortChunksBySequenceNumber%') = 3
FROM
(
    EXPLAIN PIPELINE compact = 1
    SELECT *
    FROM parallel_ordered_merge_materialization
    ORDER BY key
);

SELECT
    (
        SELECT groupArray(tuple(key, source, ids, payload, nullable_payload))
        FROM
        (
            SELECT *
            FROM parallel_ordered_merge_materialization
            ORDER BY key
            SETTINGS max_parallel_ordered_merge_materialization_threads = 4
        )
    ) =
    (
        SELECT groupArray(tuple(key, source, ids, payload, nullable_payload))
        FROM
        (
            SELECT *
            FROM parallel_ordered_merge_materialization
            ORDER BY key
            SETTINGS max_parallel_ordered_merge_materialization_threads = 0
        )
    );

DROP TABLE parallel_ordered_merge_materialization;
