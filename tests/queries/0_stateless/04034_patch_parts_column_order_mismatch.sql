-- Regression test for https://github.com/ClickHouse/clickhouse-core-incidents/issues/1021
-- When multiple patch parts (Merge + Join mode) update the same columns,
-- the column ordering in patch blocks must be deterministic to avoid
-- LOGICAL_ERROR "Block structure mismatch in patch parts stream".
--
-- The failpoint reverses column order for odd-indexed patches to expose any
-- code relying on positional column matching. Without the sort in
-- getUpdatedHeader, this triggers the bug.

SET enable_lightweight_update = 1;

SYSTEM ENABLE FAILPOINT patch_parts_reverse_column_order;

DROP TABLE IF EXISTS t_patch_order;

CREATE TABLE t_patch_order (id UInt64, a_col String, b_col UInt64, c_col Float64, d_col UInt32, e_col String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0;

-- Insert two separate blocks to create two base parts.
INSERT INTO t_patch_order VALUES (1, 'hello', 10, 1.5, 100, 'world');
INSERT INTO t_patch_order VALUES (2, 'foo', 20, 2.5, 200, 'bar');

-- First UPDATE: creates Merge-mode patch parts for both base parts.
UPDATE t_patch_order SET a_col = 'updated1', b_col = 99, c_col = 9.9, d_col = 999, e_col = 'upd1' WHERE 1;

-- Verify patch application works in Merge mode.
SELECT * FROM t_patch_order ORDER BY id;

-- Merge base parts; patches become Join-mode (apply_patches_on_merge = 0).
OPTIMIZE TABLE t_patch_order FINAL;

-- Second UPDATE: creates new Merge-mode patch parts for the merged base part.
UPDATE t_patch_order SET a_col = 'updated2', b_col = 88, c_col = 8.8, d_col = 888, e_col = 'upd2' WHERE 1;

-- This SELECT must apply both Join-mode and Merge-mode patches simultaneously.
-- The failpoint reverses column order for odd-indexed patches. Without the fix,
-- getUpdatedHeader throws LOGICAL_ERROR because it compares patch headers positionally.
SELECT * FROM t_patch_order ORDER BY id;

-- Materialize patches and verify final state.
ALTER TABLE t_patch_order APPLY PATCHES SETTINGS mutations_sync = 2;
SELECT * FROM t_patch_order ORDER BY id SETTINGS apply_patch_parts = 0;

SYSTEM DISABLE FAILPOINT patch_parts_reverse_column_order;

DROP TABLE t_patch_order;
