-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

-- A merge of wide parts that straddles a pending `RENAME COLUMN` of a `Nested` leaf writes the leaf
-- under its new name and records the rename as applied, so the merged part must keep the leaf's values.

DROP TABLE IF EXISTS t_merge_pending_rename_nested_leaf;

-- Leave the only merge to `OPTIMIZE ... FINAL`, which ignores `max_bytes_to_merge_at_max_space_in_pool`.
CREATE TABLE t_merge_pending_rename_nested_leaf (id UInt64, `n.a` Array(UInt8), `n.b` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_merge_pending_rename_nested_leaf VALUES (1, [7, 8], ['x', 'y']);
INSERT INTO t_merge_pending_rename_nested_leaf VALUES (2, [9], ['w']);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
ALTER TABLE t_merge_pending_rename_nested_leaf RENAME COLUMN `n.b` TO `n.z` SETTINGS alter_sync = 0;
OPTIMIZE TABLE t_merge_pending_rename_nested_leaf FINAL SETTINGS optimize_throw_if_noop = 1;
SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SELECT 'merged into', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_merge_pending_rename_nested_leaf' AND active;
SELECT 'rename done', is_done FROM system.mutations
WHERE database = currentDatabase() AND table = 't_merge_pending_rename_nested_leaf';
SELECT id, `n.a`, `n.z` FROM t_merge_pending_rename_nested_leaf ORDER BY id;

DROP TABLE t_merge_pending_rename_nested_leaf;
