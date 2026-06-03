-- Tags: no-parallel, no-parallel-replicas
-- Regression test: mutation on a Wide part with a non-identity column_id,
-- followed by RENAME and server restart, must not silently drop the column.
--
-- Root cause: MutateTask::finalizeMutatedPart wrote columns.txt without
-- use_column_ids=true, so the file contained the logical column name while
-- the on-disk data files were named by column ID.  After part reload,
-- remapColumnsWithPhysicalNames fell through to its identity-fallback case
-- and stamped column_id := logical_name, after which merge could not find
-- the data file and dropped the column from the output part.

SET allow_experimental_column_ids = 1;

DROP TABLE IF EXISTS t_mutate_rename_reload SYNC;

CREATE TABLE t_mutate_rename_reload (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;

-- Push c's column_id off identity: after DROP+ADD, the new c gets a numeric ID.
INSERT INTO t_mutate_rename_reload VALUES (1, 'x', 1.5);
ALTER TABLE t_mutate_rename_reload DROP COLUMN c;
ALTER TABLE t_mutate_rename_reload ADD COLUMN c Float64;
INSERT INTO t_mutate_rename_reload VALUES (2, 'y', 9.9);

-- Verify c's column_id is non-identity (numeric, not 'c').
SELECT 'before_mutation', column, column_id != column AS is_non_identity
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mutate_rename_reload' AND active AND column = 'c'
ORDER BY name;

-- Light mutation: single-column UPDATE on a Wide part triggers
-- MutateSomePartColumnsTask -> finalizeMutatedPart, which is the broken path.
ALTER TABLE t_mutate_rename_reload UPDATE c = 99.9 WHERE a = 2 SETTINGS mutations_sync = 1;

-- Rename to force the broken-columns.txt path to surface on reload.
ALTER TABLE t_mutate_rename_reload RENAME COLUMN c TO price;

-- Force part reload from disk.
DETACH TABLE t_mutate_rename_reload SYNC;
ATTACH TABLE t_mutate_rename_reload;

-- Sanity check after reload: reader uses table-level mapping, so SELECT
-- still returns correct data even if the part-level column_id is wrong.
SELECT 'after_restart', a, price FROM t_mutate_rename_reload ORDER BY a;

-- Trigger merge.  Before fix: merge uses the part-level column_id, can't
-- find the data file, drops the column entirely from the output part.
OPTIMIZE TABLE t_mutate_rename_reload FINAL;

-- Smoking gun: the column must survive the merge.
SELECT 'after_merge', column, column_id != column AS is_non_identity
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mutate_rename_reload' AND active AND column = 'price'
ORDER BY name;

SELECT 'final', a, price FROM t_mutate_rename_reload ORDER BY a;

DROP TABLE t_mutate_rename_reload SYNC;
