-- A rows-TTL merge on `CollapsingMergeTree` and `VersionedCollapsingMergeTree` may use the vertical
-- algorithm, and must return the rows a horizontal merge returns. Each key lives in one part, so a
-- single merge collapses it and applies the TTL.

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

-- Test 1: collapsing keeps up to two rows of a key - the first negative and the last positive - and
-- the TTL is applied to each of them on its own.
DROP TABLE IF EXISTS t_ttl_vert_coll;

CREATE TABLE t_ttl_vert_coll
(
    id UInt64,
    sign Int8,
    d DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- Key 1 survives, key 2 expires. Keys 3, 4, 5 and 6 emit both rows, of which the TTL takes the
-- first, the second, both and neither. Key 9 cancels out before the TTL is ever consulted.
-- A row whose sign is neither 1 nor -1 is emitted on a branch of its own, which the TTL filter now
-- covers too. It is not asserted here: a vertical merge already mismatches the gathered columns of
-- such a row against its key, with or without a TTL, so there is no stable result to pin.
INSERT INTO t_ttl_vert_coll VALUES
    (1,  1, '2100-01-01 00:00:00', 101, 201, 301),
    (2,  1, '2000-01-01 00:00:00', 102, 202, 302),
    (3, -1, '2000-01-01 00:00:00', 103, 203, 303),
    (3,  1, '2100-01-01 00:00:00', 113, 213, 313),
    (4, -1, '2100-01-01 00:00:00', 104, 204, 304),
    (4,  1, '2000-01-01 00:00:00', 114, 214, 314),
    (5, -1, '2000-01-01 00:00:00', 105, 205, 305),
    (5,  1, '2000-01-01 00:00:00', 115, 215, 315),
    (6, -1, '2100-01-01 00:00:00', 106, 206, 306),
    (6,  1, '2100-01-01 00:00:00', 116, 216, 316),
    (9,  1, '2100-01-01 00:00:00', 109, 209, 309),
    (9, -1, '2100-01-01 00:00:00', 119, 219, 319);

OPTIMIZE TABLE t_ttl_vert_coll FINAL;

SELECT 'test1_rows', id, sign, c1, c2, c3 FROM t_ttl_vert_coll ORDER BY id, sign;

SYSTEM FLUSH LOGS part_log;
SELECT 'test1_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_coll' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_coll;

-- Test 2: control - the same data with the optimization off must produce the same rows.
DROP TABLE IF EXISTS t_ttl_vert_coll_off;

CREATE TABLE t_ttl_vert_coll_off
(
    id UInt64,
    sign Int8,
    d DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 0,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_ttl_vert_coll_off VALUES
    (1,  1, '2100-01-01 00:00:00', 101, 201, 301),
    (2,  1, '2000-01-01 00:00:00', 102, 202, 302),
    (3, -1, '2000-01-01 00:00:00', 103, 203, 303),
    (3,  1, '2100-01-01 00:00:00', 113, 213, 313),
    (4, -1, '2100-01-01 00:00:00', 104, 204, 304),
    (4,  1, '2000-01-01 00:00:00', 114, 214, 314),
    (5, -1, '2000-01-01 00:00:00', 105, 205, 305),
    (5,  1, '2000-01-01 00:00:00', 115, 215, 315),
    (6, -1, '2100-01-01 00:00:00', 106, 206, 306),
    (6,  1, '2100-01-01 00:00:00', 116, 216, 316),
    (9,  1, '2100-01-01 00:00:00', 109, 209, 309),
    (9, -1, '2100-01-01 00:00:00', 119, 219, 319);

OPTIMIZE TABLE t_ttl_vert_coll_off FINAL;

SELECT 'test2_rows', id, sign, c1, c2, c3 FROM t_ttl_vert_coll_off ORDER BY id, sign;

SYSTEM FLUSH LOGS part_log;
SELECT 'test2_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_coll_off' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_coll_off;

-- Test 3: versioned collapsing pairs rows by sorting key, so the version separates rows that would
-- otherwise cancel out, and the TTL is applied to whatever survives the pairing.
DROP TABLE IF EXISTS t_ttl_vert_vcoll;

CREATE TABLE t_ttl_vert_vcoll
(
    id UInt64,
    sign Int8,
    version UInt64,
    d DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (id, version)
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- Key 1 survives, key 2 expires. Key 3 cancels out before the TTL is consulted. Keys 4 and 5 hold
-- two versions that cannot cancel, and the TTL removes one of them and then both.
INSERT INTO t_ttl_vert_vcoll VALUES
    (1,  1, 1, '2100-01-01 00:00:00', 101, 201, 301),
    (2,  1, 1, '2000-01-01 00:00:00', 102, 202, 302),
    (3,  1, 1, '2100-01-01 00:00:00', 103, 203, 303),
    (3, -1, 1, '2100-01-01 00:00:00', 113, 213, 313),
    (4,  1, 1, '2000-01-01 00:00:00', 104, 204, 304),
    (4,  1, 2, '2100-01-01 00:00:00', 114, 214, 314),
    (5, -1, 1, '2000-01-01 00:00:00', 105, 205, 305),
    (5,  1, 2, '2000-01-01 00:00:00', 115, 215, 315);

OPTIMIZE TABLE t_ttl_vert_vcoll FINAL;

SELECT 'test3_rows', id, sign, version, c1, c2, c3 FROM t_ttl_vert_vcoll ORDER BY id, version;

SYSTEM FLUSH LOGS part_log;
SELECT 'test3_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_vcoll' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_vcoll;

-- Test 4: control - the same data with the optimization off must produce the same rows.
DROP TABLE IF EXISTS t_ttl_vert_vcoll_off;

CREATE TABLE t_ttl_vert_vcoll_off
(
    id UInt64,
    sign Int8,
    version UInt64,
    d DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (id, version)
TTL d + INTERVAL 1 DAY
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 0,
    merge_with_ttl_timeout = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_ttl_vert_vcoll_off VALUES
    (1,  1, 1, '2100-01-01 00:00:00', 101, 201, 301),
    (2,  1, 1, '2000-01-01 00:00:00', 102, 202, 302),
    (3,  1, 1, '2100-01-01 00:00:00', 103, 203, 303),
    (3, -1, 1, '2100-01-01 00:00:00', 113, 213, 313),
    (4,  1, 1, '2000-01-01 00:00:00', 104, 204, 304),
    (4,  1, 2, '2100-01-01 00:00:00', 114, 214, 314),
    (5, -1, 1, '2000-01-01 00:00:00', 105, 205, 305),
    (5,  1, 2, '2000-01-01 00:00:00', 115, 215, 315);

OPTIMIZE TABLE t_ttl_vert_vcoll_off FINAL;

SELECT 'test4_rows', id, sign, version, c1, c2, c3 FROM t_ttl_vert_vcoll_off ORDER BY id, version;

SYSTEM FLUSH LOGS part_log;
SELECT 'test4_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_vcoll_off' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_vcoll_off;
