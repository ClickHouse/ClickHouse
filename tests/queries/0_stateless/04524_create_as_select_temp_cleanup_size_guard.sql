-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- ^ A plain `CREATE TABLE ... AS SELECT` uses the temporary-create-then-publish path only for Atomic
--   databases; the size-to-drop accounting is specific to a plain (non-shared) MergeTree.

-- A plain `CREATE TABLE ... AS SELECT` whose populating INSERT SELECT fails *after* the internal
-- temporary table has already been filled must still drop that temporary table on the failure path,
-- even under a strict `max_table_size_to_drop`. The temporary table is an internal implementation
-- detail, so its cleanup DROP must bypass the size guard; otherwise a filled-then-abandoned temporary
-- table would be stranded as `_tmp_replace_*` (this mirrors CREATE OR REPLACE, see
-- 04326_create_or_replace_size_check_pre_flight).

DROP TABLE IF EXISTS dst_04524;

-- One row per block, no squashing, so blocks 0..49 are written as parts into the temporary MergeTree
-- table before `throwIf` fires on row 50. By then the temporary table holds data and exceeds
-- `max_table_size_to_drop = 1`, so the cleanup DROP would fail with
-- `TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT` and strand `_tmp_replace_*` unless it bypasses the guard.
CREATE TABLE dst_04524 (a UInt64) ENGINE = MergeTree ORDER BY a
AS SELECT throwIf(number = 50, 'stop') AS a FROM numbers(100)
SETTINGS max_table_size_to_drop = 1, max_insert_block_size = 1,
         min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         max_block_size = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The failing query must leave no orphan target table ...
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dst_04524';
-- ... and must not strand the internal temporary table.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

-- Sanity: a strict `max_table_size_to_drop` must not interfere with the success path (on success the
-- temporary table is published via a plain RENAME and is never dropped).
CREATE TABLE dst_04524_ok (a UInt64) ENGINE = MergeTree ORDER BY a
AS SELECT number AS a FROM numbers(100)
SETTINGS max_table_size_to_drop = 1;

SELECT count() FROM dst_04524_ok;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

DROP TABLE dst_04524_ok;
