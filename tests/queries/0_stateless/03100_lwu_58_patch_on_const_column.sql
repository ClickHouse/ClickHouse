-- A patch part written after a pending `ALTER TABLE ... UPDATE` must win over it, also when the
-- `ALTER` assigns a constant to every row. Such an assignment is constant-folded, so the column
-- that the on-fly mutation hands to the patch application is a `ColumnConst`, which has to be
-- materialized before the patch is written into it.
-- Several rows are inserted and only a subset is patched, so that materializing the constant has
-- to replicate it to the full block and leave the unpatched rows at the `ALTER`'s value.

DROP TABLE IF EXISTS t_lwu_const SYNC;

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

CREATE TABLE t_lwu_const (id UInt64, i Int32, fs FixedString(3), s String, n Nullable(Int32), lc LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    shared_merge_tree_disable_merges_and_mutations_assignment = 1,
    patch_parts_version = 'v2';

INSERT INTO t_lwu_const SELECT number, 10, 'aaa', 'a', 10, 'a' FROM numbers(5);

SYSTEM STOP MERGES t_lwu_const;

ALTER TABLE t_lwu_const UPDATE i = 20, fs = 'bbb', s = 'b', n = 20, lc = 'b' WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;

SET apply_mutations_on_fly = 0;
UPDATE t_lwu_const SET i = 99, fs = 'ccc', s = 'c', n = 99, lc = 'c' WHERE id IN (1, 3);

-- The `ALTER` must still be unmaterialized, otherwise there is nothing to apply on the fly.
SELECT 'pending mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lwu_const' AND NOT is_done;

SELECT 'patch only', id, i, fs, s, n, lc FROM t_lwu_const ORDER BY id;

SET apply_mutations_on_fly = 1;

-- Columns that are not fixed-and-contiguous take the copying patch path, where
-- `ColumnConst::insertFrom` used to discard the patched value.
SELECT 'on fly var', id, s, n, lc FROM t_lwu_const ORDER BY id;

-- Fixed-width columns take the in-place patch path, which used to reach `IColumn::updateAt`.
SELECT 'on fly fixed', id, i, fs FROM t_lwu_const ORDER BY id;

SELECT 'on fly all', id, i, fs, s, n, lc FROM t_lwu_const ORDER BY id;

DROP TABLE t_lwu_const SYNC;
