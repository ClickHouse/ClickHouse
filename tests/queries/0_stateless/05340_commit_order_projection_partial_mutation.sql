-- A mutation that rewrites only some columns of a Wide part builds the missing commit-order projection from the part's real data.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_d;
DROP TABLE IF EXISTS t_d_src;
DROP TABLE IF EXISTS t_e;

-- A: UPDATE of one column on a level-0 part.
CREATE TABLE t_a (id UInt64, g UInt64, w UInt64,
    PROJECTION p2 (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_a SELECT number, number % 7, number FROM numbers(10);
ALTER TABLE t_a UPDATE w = w + 1 WHERE id = 5;
SELECT 'A', (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_a, p2)),
    (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM mergeTreeProjection(currentDatabase(), t_a, p2))
    = (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM t_a SETTINGS optimize_use_projections = 0);
SELECT id, g, w FROM t_a ORDER BY _block_number, _block_offset;

-- B: MATERIALIZE PROJECTION of another projection.
CREATE TABLE t_b (id UInt64, g UInt64, w UInt64,
    PROJECTION p2 (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_b SELECT number, number % 7, number FROM numbers(10);
ALTER TABLE t_b ADD PROJECTION p (SELECT id, g ORDER BY g);
ALTER TABLE t_b MATERIALIZE PROJECTION p;
SELECT 'B', (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_b, p2)),
    (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM mergeTreeProjection(currentDatabase(), t_b, p2))
    = (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM t_b SETTINGS optimize_use_projections = 0);

-- C: the part stores _block_number after a merge, and the projection is added later.
CREATE TABLE t_c (id UInt64, g UInt64, w UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_c SELECT number, number % 7, number FROM numbers(10);
OPTIMIZE TABLE t_c FINAL;
ALTER TABLE t_c ADD PROJECTION p2 (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset);
ALTER TABLE t_c UPDATE w = w + 1 WHERE 1;
SELECT 'C', (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_c, p2)),
    (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM mergeTreeProjection(currentDatabase(), t_c, p2))
    = (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM t_c SETTINGS optimize_use_projections = 0);

-- D: a merged part attached from another table.
CREATE TABLE t_d_src (id UInt64, g UInt64, w UInt64,
    PROJECTION p2 (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
CREATE TABLE t_d AS t_d_src;
INSERT INTO t_d_src SELECT number, number % 7, number FROM numbers(10);
OPTIMIZE TABLE t_d_src FINAL;
ALTER TABLE t_d ATTACH PARTITION tuple() FROM t_d_src;
ALTER TABLE t_d UPDATE w = w + 1 WHERE 1;
SELECT 'D', (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_d, p2)),
    (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM mergeTreeProjection(currentDatabase(), t_d, p2))
    = (SELECT arraySort(groupArray((id, g, w, _block_number, _block_offset))) FROM t_d SETTINGS optimize_use_projections = 0);

-- E: a part with lightweight-deleted rows. The rebuilt projection may still keep the deleted rows, so it is checked to hold every surviving row.
CREATE TABLE t_e (id UInt64, g UInt64, w UInt64, PROJECTION p (SELECT id, g ORDER BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild',
    enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_e SELECT number, number % 7, number FROM numbers(10);
DELETE FROM t_e WHERE id < 3 SETTINGS lightweight_deletes_sync = 2;
ALTER TABLE t_e ADD PROJECTION p2 (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset);
ALTER TABLE t_e UPDATE w = w + 1 WHERE 1;
SELECT 'E', count(), arraySort(groupArray((id, g, w))) FROM t_e SETTINGS optimize_use_projections = 0;
SELECT 'E', hasAll(
    (SELECT groupArray((id, g, w, _block_number, _block_offset)) FROM mergeTreeProjection(currentDatabase(), t_e, p2)),
    (SELECT groupArray((id, g, w, _block_number, _block_offset)) FROM t_e SETTINGS optimize_use_projections = 0));

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_d;
DROP TABLE t_d_src;
DROP TABLE t_e;
