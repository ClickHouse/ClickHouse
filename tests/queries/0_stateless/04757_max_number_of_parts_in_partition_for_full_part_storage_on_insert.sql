CREATE TABLE t_level_0 (p UInt8, id UInt64)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0,
    max_number_of_parts_in_partition_for_full_part_storage_on_insert = 3;

SYSTEM STOP MERGES t_level_0;

INSERT INTO t_level_0 VALUES (0, 0);
INSERT INTO t_level_0 VALUES (0, 1);
INSERT INTO t_level_0 VALUES (0, 2);
INSERT INTO t_level_0 VALUES (0, 3);
INSERT INTO t_level_0 VALUES (1, 4);

SELECT partition, level, part_storage_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_level_0' AND active
ORDER BY partition, min_block_number;

SET optimize_on_insert = 1;

CREATE TABLE t_level_1
(
    p UInt8,
    id UInt64,
    PROJECTION projection_by_id (SELECT p, id ORDER BY id)
)
ENGINE = ReplacingMergeTree
PARTITION BY p
ORDER BY id
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0,
    max_number_of_parts_in_partition_for_full_part_storage_on_insert = 1;

SYSTEM STOP MERGES t_level_1;

INSERT INTO t_level_1 VALUES (0, 0);
INSERT INTO t_level_1 VALUES (0, 1);
INSERT INTO t_level_1 VALUES (1, 2);

SELECT partition, level, part_storage_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_level_1' AND active
ORDER BY partition, min_block_number;

SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_level_1' AND active;

CREATE TABLE t_mv_source (p UInt8, id UInt64)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id;

CREATE TABLE t_mv_target (p UInt8, id UInt64)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0,
    max_number_of_parts_in_partition_for_full_part_storage_on_insert = 1;

SYSTEM STOP MERGES t_mv_target;

INSERT INTO t_mv_target VALUES (0, 0);
CREATE MATERIALIZED VIEW t_mv TO t_mv_target AS SELECT p, id FROM t_mv_source;
INSERT INTO t_mv_source VALUES (0, 1);

SELECT partition, level, part_storage_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_mv_target' AND active
ORDER BY partition, min_block_number;

CREATE TABLE t_non_polymorphic (p UInt8, id UInt64)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS
    index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0,
    max_number_of_parts_in_partition_for_full_part_storage_on_insert = 1;

SYSTEM STOP MERGES t_non_polymorphic;

INSERT INTO t_non_polymorphic VALUES (0, 0);
INSERT INTO t_non_polymorphic VALUES (0, 1);

SELECT partition, level, part_storage_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_non_polymorphic' AND active
ORDER BY partition, min_block_number;

DROP VIEW t_mv;
DROP TABLE t_mv_source;
DROP TABLE t_mv_target;
DROP TABLE t_level_0;
DROP TABLE t_level_1;
DROP TABLE t_non_polymorphic;
