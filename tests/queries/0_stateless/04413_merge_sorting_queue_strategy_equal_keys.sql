DROP TABLE IF EXISTS source_04413;
DROP TABLE IF EXISTS equal_keys_default_04413;
DROP TABLE IF EXISTS equal_keys_batch_04413;

CREATE TABLE source_04413
(
    id UInt64,
    k UInt8,
    payload String
)
ENGINE = Memory;

CREATE TABLE equal_keys_default_04413 AS source_04413
ENGINE = MergeTree
ORDER BY k
SETTINGS
    merge_use_batch_sorting_queue = 0,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

CREATE TABLE equal_keys_batch_04413 AS equal_keys_default_04413
ENGINE = MergeTree
ORDER BY k
SETTINGS
    merge_use_batch_sorting_queue = 1,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO source_04413
SELECT number, toUInt8(number % 3), concat('payload-', toString(number))
FROM numbers(96);

INSERT INTO equal_keys_default_04413 SELECT * FROM source_04413 WHERE id % 3 = 0;
INSERT INTO equal_keys_default_04413 SELECT * FROM source_04413 WHERE id % 3 = 1;
INSERT INTO equal_keys_default_04413 SELECT * FROM source_04413 WHERE id % 3 = 2;

INSERT INTO equal_keys_batch_04413 SELECT * FROM source_04413 WHERE id % 3 = 0;
INSERT INTO equal_keys_batch_04413 SELECT * FROM source_04413 WHERE id % 3 = 1;
INSERT INTO equal_keys_batch_04413 SELECT * FROM source_04413 WHERE id % 3 = 2;

OPTIMIZE TABLE equal_keys_default_04413 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE equal_keys_batch_04413 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT throwIf(
    (
        SELECT groupArray(tuple(id, k, payload, _block_number, _block_offset))
        FROM (SELECT id, k, payload, _block_number, _block_offset FROM equal_keys_default_04413 ORDER BY k, _part_offset)
    ) != (
        SELECT groupArray(tuple(id, k, payload, _block_number, _block_offset))
        FROM (SELECT id, k, payload, _block_number, _block_offset FROM equal_keys_batch_04413 ORDER BY k, _part_offset)
    ),
    'Equal-key merge order or row identity differs between default and batch sorting queues')
FORMAT Null;

SELECT 'equal keys ok';

DROP TABLE equal_keys_default_04413;
DROP TABLE equal_keys_batch_04413;
DROP TABLE source_04413;
