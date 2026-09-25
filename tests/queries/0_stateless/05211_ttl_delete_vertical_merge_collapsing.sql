-- A rows-TTL merge on `CollapsingMergeTree` and `VersionedCollapsingMergeTree` may use the vertical
-- algorithm, and must return the rows a horizontal merge returns. Each key lives in one part, so a
-- single merge collapses it and applies the TTL.
--
-- Every case carries its control: a clone of the table with `vertical_merge_optimize_ttl_delete`
-- off, filled from the case table so neither the schema nor the rows can drift apart, and asserted
-- to hold the same rows afterwards. `_control_algo` is `Horizontal` - that is what makes it a
-- control rather than a second copy of the case.

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

-- Test 1: collapsing keeps up to two rows of a key - the first negative and the last positive - and
-- the TTL is applied to each of them on its own.
DROP TABLE IF EXISTS t_ttl_vert_coll;
DROP TABLE IF EXISTS t_ttl_vert_coll_off;

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

CREATE TABLE t_ttl_vert_coll_off AS t_ttl_vert_coll;
ALTER TABLE t_ttl_vert_coll_off MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

-- Key 1 survives, key 2 expires. Keys 3, 4, 5 and 6 emit both rows, of which the TTL takes the
-- first, the second, both and neither. Key 9 cancels out before the TTL is ever consulted.
-- A row whose sign is neither 1 nor -1 is kept on a branch of its own, which the TTL filter covers
-- too. Keys 7, 8, 10 and 11 put such a row before, between and after the rows collapsing selects,
-- and expire it in key 10 - the positions differ because a vertical merge has to emit a key in read
-- order, so a kept invalid row must reach the output between the selected rows it was read between.
-- A background TTL merge would delete rows before the control copies them.
SYSTEM STOP TTL MERGES t_ttl_vert_coll;

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
    (9, -1, '2100-01-01 00:00:00', 119, 219, 319),
    (7,  0, '2100-01-01 00:00:00', 107, 207, 307),
    (7, -1, '2000-01-01 00:00:00', 117, 217, 317),
    (7,  1, '2100-01-01 00:00:00', 127, 227, 327),
    (8, -1, '2100-01-01 00:00:00', 108, 208, 308),
    (8,  0, '2100-01-01 00:00:00', 118, 218, 318),
    (8,  1, '2000-01-01 00:00:00', 128, 228, 328),
    (10, 0, '2000-01-01 00:00:00', 110, 210, 310),
    (10, 1, '2100-01-01 00:00:00', 120, 220, 320),
    (11, 1, '2100-01-01 00:00:00', 111, 211, 311),
    (11, 0, '2100-01-01 00:00:00', 121, 221, 321);

INSERT INTO t_ttl_vert_coll_off SELECT * FROM t_ttl_vert_coll;
SYSTEM START TTL MERGES t_ttl_vert_coll;

OPTIMIZE TABLE t_ttl_vert_coll FINAL;
OPTIMIZE TABLE t_ttl_vert_coll_off FINAL;

SELECT 'test1_rows', id, sign, c1, c2, c3 FROM t_ttl_vert_coll ORDER BY id, c1;
SELECT 'test1_control_same_rows', arraySort(groupArray((id, sign, d, c1, c2, c3)))
    = (SELECT arraySort(groupArray((id, sign, d, c1, c2, c3))) FROM t_ttl_vert_coll_off)
FROM t_ttl_vert_coll;

SYSTEM FLUSH LOGS part_log;
SELECT 'test1_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_coll' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;
SELECT 'test1_control_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_coll_off' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_coll;
DROP TABLE t_ttl_vert_coll_off;

-- Test 2: versioned collapsing pairs rows by sorting key, so the version separates rows that would
-- otherwise cancel out, and the TTL is applied to whatever survives the pairing.
DROP TABLE IF EXISTS t_ttl_vert_vcoll;
DROP TABLE IF EXISTS t_ttl_vert_vcoll_off;

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

CREATE TABLE t_ttl_vert_vcoll_off AS t_ttl_vert_vcoll;
ALTER TABLE t_ttl_vert_vcoll_off MODIFY SETTING vertical_merge_optimize_ttl_delete = 0;

-- Key 1 survives, key 2 expires. Key 3 cancels out before the TTL is consulted. Keys 4 and 5 hold
-- two versions that cannot cancel, and the TTL removes one of them and then both.
SYSTEM STOP TTL MERGES t_ttl_vert_vcoll;

INSERT INTO t_ttl_vert_vcoll VALUES
    (1,  1, 1, '2100-01-01 00:00:00', 101, 201, 301),
    (2,  1, 1, '2000-01-01 00:00:00', 102, 202, 302),
    (3,  1, 1, '2100-01-01 00:00:00', 103, 203, 303),
    (3, -1, 1, '2100-01-01 00:00:00', 113, 213, 313),
    (4,  1, 1, '2000-01-01 00:00:00', 104, 204, 304),
    (4,  1, 2, '2100-01-01 00:00:00', 114, 214, 314),
    (5, -1, 1, '2000-01-01 00:00:00', 105, 205, 305),
    (5,  1, 2, '2000-01-01 00:00:00', 115, 215, 315);

INSERT INTO t_ttl_vert_vcoll_off SELECT * FROM t_ttl_vert_vcoll;
SYSTEM START TTL MERGES t_ttl_vert_vcoll;

OPTIMIZE TABLE t_ttl_vert_vcoll FINAL;
OPTIMIZE TABLE t_ttl_vert_vcoll_off FINAL;

SELECT 'test2_rows', id, sign, version, c1, c2, c3 FROM t_ttl_vert_vcoll ORDER BY id, version;
SELECT 'test2_control_same_rows', arraySort(groupArray((id, sign, version, d, c1, c2, c3)))
    = (SELECT arraySort(groupArray((id, sign, version, d, c1, c2, c3))) FROM t_ttl_vert_vcoll_off)
FROM t_ttl_vert_vcoll;

SYSTEM FLUSH LOGS part_log;
SELECT 'test2_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_vcoll' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;
SELECT 'test2_control_algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_vcoll_off' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_vcoll;
DROP TABLE t_ttl_vert_vcoll_off;
