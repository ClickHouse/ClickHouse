DROP TABLE IF EXISTS source_04614;
DROP TABLE IF EXISTS projection_default_04614;
DROP TABLE IF EXISTS projection_batch_04614;

CREATE TABLE source_04614
(
    id UInt64,
    k UInt8,
    payload String
)
ENGINE = Memory;

CREATE TABLE projection_default_04614
(
    id UInt64,
    k UInt8,
    payload String,
    PROJECTION p
    (
        SELECT id, payload, _part_offset
        ORDER BY payload
    )
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    merge_sorting_queue_strategy = 'default',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

CREATE TABLE projection_batch_04614
(
    id UInt64,
    k UInt8,
    payload String,
    PROJECTION p
    (
        SELECT id, payload, _part_offset
        ORDER BY payload
    )
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    merge_sorting_queue_strategy = 'batch',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO source_04614
SELECT number, toUInt8(number % 3), concat('payload-', toString(number))
FROM numbers(256);

INSERT INTO projection_default_04614 SELECT * FROM source_04614 WHERE id % 4 = 0;
INSERT INTO projection_default_04614 SELECT * FROM source_04614 WHERE id % 4 = 1;
INSERT INTO projection_default_04614 SELECT * FROM source_04614 WHERE id % 4 = 2;
INSERT INTO projection_default_04614 SELECT * FROM source_04614 WHERE id % 4 = 3;

INSERT INTO projection_batch_04614 SELECT * FROM source_04614 WHERE id % 4 = 0;
INSERT INTO projection_batch_04614 SELECT * FROM source_04614 WHERE id % 4 = 1;
INSERT INTO projection_batch_04614 SELECT * FROM source_04614 WHERE id % 4 = 2;
INSERT INTO projection_batch_04614 SELECT * FROM source_04614 WHERE id % 4 = 3;

OPTIMIZE TABLE projection_default_04614 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE projection_batch_04614 FINAL SETTINGS optimize_throw_if_noop = 1;

WITH
    (
        SELECT groupArray(tuple(id, payload, _part_offset))
        FROM (SELECT id, payload, _part_offset FROM projection_default_04614 ORDER BY id)
    ) AS default_parent,
    (
        SELECT groupArray(tuple(id, payload, _parent_part_offset))
        FROM
        (
            SELECT id, payload, _parent_part_offset
            FROM mergeTreeProjection(currentDatabase(), 'projection_default_04614', 'p')
            ORDER BY id
        )
    ) AS default_projection,
    (
        SELECT groupArray(tuple(id, payload, _part_offset))
        FROM (SELECT id, payload, _part_offset FROM projection_batch_04614 ORDER BY id)
    ) AS batch_parent,
    (
        SELECT groupArray(tuple(id, payload, _parent_part_offset))
        FROM
        (
            SELECT id, payload, _parent_part_offset
            FROM mergeTreeProjection(currentDatabase(), 'projection_batch_04614', 'p')
            ORDER BY id
        )
    ) AS batch_projection
SELECT throwIf(
    default_parent != default_projection
        OR batch_parent != batch_projection
        OR default_projection != batch_projection,
    'Projection parent offsets differ between default and batch sorting queue strategies')
FORMAT Null;

SELECT 'projection offsets ok';

DROP TABLE projection_default_04614;
DROP TABLE projection_batch_04614;
DROP TABLE source_04614;
