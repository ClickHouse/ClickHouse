-- Tags: no-parallel, no-parallel-replicas
-- Regression: a wide-part partial mutation following a metadata-only
-- RENAME (without DETACH/ATTACH) must not drop the renamed column.
-- Before the fix, getColumnsForNewDataPart keyed source-slot lookups by
-- the part's load-time col.name, which was the pre-rename name, so the
-- storage iteration missed the renamed column and erased it.

SET allow_experimental_column_ids = 1;

DROP TABLE IF EXISTS t_mutate_after_rename SYNC;

CREATE TABLE t_mutate_after_rename (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;

INSERT INTO t_mutate_after_rename VALUES (1, 'hello', 1.5), (2, 'world', 2.5);

-- Metadata-only RENAME with no DETACH/ATTACH afterwards.  The active
-- part's cached `columns` list still has col.name = 'b' (pre-rename),
-- while col.column_id remains 'b' and the current mapping has d -> 'b'.
ALTER TABLE t_mutate_after_rename RENAME COLUMN b TO d;

-- Partial mutation that doesn't touch 'd' (UPDATE on non-key, non-renamed
-- column 'c').  Drives the wide-part stale-`col.name` path in
-- getColumnsForNewDataPart.
ALTER TABLE t_mutate_after_rename UPDATE c = c + 100 WHERE 1 SETTINGS mutations_sync = 1;

-- Smoking gun: renamed 'd' must still carry the original String data.
SELECT 'after_mutation', a, d, c FROM t_mutate_after_rename ORDER BY a;

-- The active part must list 'd' (renamed column survived in columns.txt).
SELECT 'columns', column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mutate_after_rename' AND active
ORDER BY column;

-- Force a merge so the post-mutation part is rewritten once more.
INSERT INTO t_mutate_after_rename (a, c, d) VALUES (3, 3.5, 'after');
OPTIMIZE TABLE t_mutate_after_rename FINAL;
SELECT 'after_merge', a, d, c FROM t_mutate_after_rename ORDER BY a;

DROP TABLE t_mutate_after_rename SYNC;
