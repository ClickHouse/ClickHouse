-- Regression test: lightweight update values must NOT be silently dropped
-- when a pending ALTER MODIFY COLUMN changes the column type.
-- A naive fix that skips mismatched-type patches causes data loss:
-- the user sees NULL instead of the converted value.

DROP TABLE IF EXISTS t_lwu_patch_dataloss SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_patch_dataloss (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_patch_dataloss VALUES (1, 'a'), (2, 'b'), (3, 'c');

ALTER TABLE t_lwu_patch_dataloss ADD COLUMN col1 Nullable(Int64);

SYSTEM STOP MERGES t_lwu_patch_dataloss;

-- Lightweight UPDATE writes col1 = 42 as Nullable(Int64) into a patch part.
UPDATE t_lwu_patch_dataloss SET col1 = 42 WHERE id = 1;

-- Change col1 type. The mutation cannot run because merges are stopped.
ALTER TABLE t_lwu_patch_dataloss MODIFY COLUMN col1 Nullable(String) SETTINGS alter_sync = 0;

-- The patch has Nullable(Int64) 42, the result column is Nullable(String).
-- A correct fix casts 42 -> '42'. A skip-based fix silently returns NULL.
SELECT id, col1 FROM t_lwu_patch_dataloss ORDER BY id;

SYSTEM START MERGES t_lwu_patch_dataloss;
DROP TABLE t_lwu_patch_dataloss SYNC;
