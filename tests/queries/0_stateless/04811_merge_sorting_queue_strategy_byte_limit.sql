-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS merge_queue_default_04811;
DROP TABLE IF EXISTS merge_queue_batch_04811;

CREATE TABLE merge_queue_default_04811
(
    id UInt64,
    payload String,
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    merge_sorting_queue_strategy = 'default',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 1000,
    index_granularity_bytes = 4096,
    merge_max_block_size = 1000,
    merge_max_block_size_bytes = 4096,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    use_const_adaptive_granularity = 0;

CREATE TABLE merge_queue_batch_04811 AS merge_queue_default_04811
ENGINE = MergeTree
ORDER BY id
SETTINGS
    merge_sorting_queue_strategy = 'batch',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 1000,
    index_granularity_bytes = 4096,
    merge_max_block_size = 1000,
    merge_max_block_size_bytes = 4096,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    use_const_adaptive_granularity = 0;

INSERT INTO merge_queue_default_04811
SELECT number, repeat('a', 2048), number FROM numbers(100);
INSERT INTO merge_queue_default_04811
SELECT number + 50, repeat('b', 2048), number + 50 FROM numbers(100);

INSERT INTO merge_queue_batch_04811
SELECT number, repeat('a', 2048), number FROM numbers(100);
INSERT INTO merge_queue_batch_04811
SELECT number + 50, repeat('b', 2048), number + 50 FROM numbers(100);

OPTIMIZE TABLE merge_queue_default_04811 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE merge_queue_batch_04811 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT
    (
        SELECT groupArray(tuple(mark_number, rows_in_granule, id))
        FROM
        (
            SELECT mark_number, rows_in_granule, id
            FROM mergeTreeIndex(currentDatabase(), merge_queue_default_04811)
            ORDER BY mark_number
        )
    ) = (
        SELECT groupArray(tuple(mark_number, rows_in_granule, id))
        FROM
        (
            SELECT mark_number, rows_in_granule, id
            FROM mergeTreeIndex(currentDatabase(), merge_queue_batch_04811)
            ORDER BY mark_number
        )
    );

DROP TABLE merge_queue_default_04811;
DROP TABLE merge_queue_batch_04811;
