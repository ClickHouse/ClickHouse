-- Regression test for the `current_version != existing_version` abort in `PatchJoinCache`
-- (issue #107501). A mutation that does not touch a part clones it via hardlinks, and the
-- clone used to lose `invalidated_system_columns.txt`, resurrecting the stale physically
-- stored `_block_number`/`_block_offset` of a part adopted from another table. Two adopted
-- clones of the same source part then exposed duplicate row identities, and a lightweight
-- update captured both duplicates into one patch part with a constant `_part_data_version`.
--
-- The scenario is checked twice: with `Full` part storage (the clone copies or hardlinks the
-- whole part directory) and with `Packed` part storage (`freeze`/`freezeRemote` copy only the
-- data archive, so the clone inherits nothing and `cloneAndLoadDataPart` has to re-emit the
-- invalidated set explicitly).

SET enable_lightweight_update = 1;
SET mutations_sync = 2;
SET apply_patch_parts = 1;

-- Full part storage.

DROP TABLE IF EXISTS t_lwu_inv_src;
DROP TABLE IF EXISTS t_lwu_inv_dst;

CREATE TABLE t_lwu_inv_src (p UInt8, x UInt64, y UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_full_part_storage = 0;

CREATE TABLE t_lwu_inv_dst (p UInt8, x UInt64, y UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v1',
         max_bytes_to_merge_at_max_space_in_pool = 1,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_lwu_inv_src VALUES (1, 1, 0);
INSERT INTO t_lwu_inv_src VALUES (1, 2, 0);

-- Persist _block_number/_block_offset physically (they are written on a real merge).
OPTIMIZE TABLE t_lwu_inv_src PARTITION 1 FINAL;

-- Two adopted clones of the same source part; each gets invalidated_system_columns.txt,
-- which regenerates the identities, so they stay unique within the table.
ALTER TABLE t_lwu_inv_dst REPLACE PARTITION 1 FROM t_lwu_inv_src;
ALTER TABLE t_lwu_inv_dst ATTACH PARTITION 1 FROM t_lwu_inv_src;

SELECT DISTINCT part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_inv_dst' AND active;

SELECT count() == uniqExact(_block_number, _block_offset) FROM t_lwu_inv_dst;

-- A mutation that does not touch partition 1 takes the untouched-part clone path.
-- The clone must keep invalidated_system_columns.txt, otherwise the stale persisted
-- identities of the two adopted parts reappear and collide.
ALTER TABLE t_lwu_inv_dst UPDATE y = y + 1 WHERE p = 2;

SELECT count() == uniqExact(_block_number, _block_offset) FROM t_lwu_inv_dst;

-- A lightweight update writes one patch part with a constant data version.
UPDATE t_lwu_inv_dst SET y = y + 100 WHERE 1;

-- Merge the patch's source parts away, so the next read applies the patch in Join mode.
OPTIMIZE TABLE t_lwu_inv_dst PARTITION 1 FINAL;

SELECT p, x, y FROM t_lwu_inv_dst ORDER BY x, y;

DROP TABLE t_lwu_inv_dst;
DROP TABLE t_lwu_inv_src;

-- Packed part storage: only `cloneAndLoadDataPart` can carry the invalidated set over.

DROP TABLE IF EXISTS t_lwu_inv_packed_src;
DROP TABLE IF EXISTS t_lwu_inv_packed_dst;

CREATE TABLE t_lwu_inv_packed_src (p UInt8, x UInt64, y UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         min_bytes_for_full_part_storage = '100G';

CREATE TABLE t_lwu_inv_packed_dst (p UInt8, x UInt64, y UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v1',
         max_bytes_to_merge_at_max_space_in_pool = 1,
         min_bytes_for_full_part_storage = '100G';

INSERT INTO t_lwu_inv_packed_src VALUES (1, 1, 0);
INSERT INTO t_lwu_inv_packed_src VALUES (1, 2, 0);

OPTIMIZE TABLE t_lwu_inv_packed_src PARTITION 1 FINAL;

ALTER TABLE t_lwu_inv_packed_dst REPLACE PARTITION 1 FROM t_lwu_inv_packed_src;
ALTER TABLE t_lwu_inv_packed_dst ATTACH PARTITION 1 FROM t_lwu_inv_packed_src;

SELECT DISTINCT part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_inv_packed_dst' AND active;

SELECT count() == uniqExact(_block_number, _block_offset) FROM t_lwu_inv_packed_dst;

ALTER TABLE t_lwu_inv_packed_dst UPDATE y = y + 1 WHERE p = 2;

SELECT count() == uniqExact(_block_number, _block_offset) FROM t_lwu_inv_packed_dst;

UPDATE t_lwu_inv_packed_dst SET y = y + 100 WHERE 1;

OPTIMIZE TABLE t_lwu_inv_packed_dst PARTITION 1 FINAL;

SELECT p, x, y FROM t_lwu_inv_packed_dst ORDER BY x, y;

DROP TABLE t_lwu_inv_packed_dst;
DROP TABLE t_lwu_inv_packed_src;
