-- Companion of 03100_lwu_58_patch_on_const_column for the path that combines several patches into
-- one before applying it, which is a separate implementation of the same step.
-- Two lightweight updates of the same set of columns produce two patch parts, and a
-- non-fixed-width column among them (`s`) is what selects the combining path, so this covers the
-- `ColumnConst` materialization there as well. `patch_parts_version = 'v1'` is required because
-- only the legacy patch part format reaches that path.
-- The two updates touch overlapping but different row subsets, so the combined patch also has to
-- resolve versions per row and leave the rows it does not cover at the `ALTER`'s value.

DROP TABLE IF EXISTS t_lwu_const_combined SYNC;

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

CREATE TABLE t_lwu_const_combined (id UInt64, i Int32, fs FixedString(3), s String, n Nullable(Int32), lc LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    shared_merge_tree_disable_merges_and_mutations_assignment = 1,
    patch_parts_version = 'v1';

INSERT INTO t_lwu_const_combined SELECT number, 10, 'aaa', 'a', 10, 'a' FROM numbers(5);

SYSTEM STOP MERGES t_lwu_const_combined;

ALTER TABLE t_lwu_const_combined UPDATE i = 20, fs = 'bbb', s = 'b', n = 20, lc = 'b' WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;

SET apply_mutations_on_fly = 0;
UPDATE t_lwu_const_combined SET i = 98, fs = 'bbc', s = 'd', n = 98, lc = 'd' WHERE id IN (1, 2);
UPDATE t_lwu_const_combined SET i = 99, fs = 'ccc', s = 'c', n = 99, lc = 'c' WHERE id IN (2, 3);

-- The `ALTER` must still be unmaterialized, otherwise there is nothing to apply on the fly.
SELECT 'pending mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lwu_const_combined' AND NOT is_done;

SELECT 'patch only', id, i, fs, s, n, lc FROM t_lwu_const_combined ORDER BY id;

SET apply_mutations_on_fly = 1;
SELECT 'on fly var', id, s, n, lc FROM t_lwu_const_combined ORDER BY id;
SELECT 'on fly fixed', id, i, fs FROM t_lwu_const_combined ORDER BY id;
SELECT 'on fly all', id, i, fs, s, n, lc FROM t_lwu_const_combined ORDER BY id;

DROP TABLE t_lwu_const_combined SYNC;
