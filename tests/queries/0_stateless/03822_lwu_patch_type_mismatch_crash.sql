DROP TABLE IF EXISTS t_lwu_patch_type_crash SYNC;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_patch_type_crash (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_patch_type_crash VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Metadata-only: the existing part does NOT physically have this column.
ALTER TABLE t_lwu_patch_type_crash ADD COLUMN col1 Nullable(Int64);

-- Prevent mutation application so ALTER MODIFY COLUMN cannot convert parts.
SYSTEM STOP MERGES t_lwu_patch_type_crash;

-- Lightweight UPDATE creates a patch part with col1 as Nullable(Int64).
UPDATE t_lwu_patch_type_crash SET col1 = 42 WHERE id = 1;

-- Change column type. Creates a pending mutation that cannot run.
ALTER TABLE t_lwu_patch_type_crash MODIFY COLUMN col1 Nullable(String) SETTINGS alter_sync = 0;

-- fillMissingColumns fills col1 with Nullable(String) defaults,
-- the patch has col1 as Nullable(Int64), type mismatch -> SIGSEGV without the fix.
SELECT id, col1 FROM t_lwu_patch_type_crash ORDER BY id;

SYSTEM START MERGES t_lwu_patch_type_crash;
DROP TABLE t_lwu_patch_type_crash SYNC;
