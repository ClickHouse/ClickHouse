-- `MODIFY COLUMN` commits the metadata change immediately, so killing its mutation leaves the table
-- metadata permanently ahead of the part. A later mutation that only hardlinks this column records the
-- type the part really holds, and reading the column back must agree with that record.
--
-- `SYSTEM STOP MERGES` keeps the `MODIFY COLUMN` mutation queued long enough to kill it; the
-- `DROP COLUMN` that follows is synchronous, so the new part is produced within the test.
--
-- Only a wide part in full storage is hardlinked column by column. Any other shape makes the mutation
-- rewrite every column through the interpreter and record the type in metadata instead, which is the
-- path 04653 covers. Both thresholds are randomized by the test runner, so pin them here and assert
-- the resulting shape below.

-- `Nested::collect` runs only when `share_nested_offsets` is on, so pin that too.

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_killed_modify_nested;

CREATE TABLE t_killed_modify_nested (id UInt8, arr Nested(n String, i UInt64, s String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, share_nested_offsets = 1,
         auto_statistics_types = '';

INSERT INTO t_killed_modify_nested VALUES (1, ['a', 'b'], [10, 20], ['s1', 's2']);

SYSTEM STOP MERGES t_killed_modify_nested;

SET alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_killed_modify_nested MODIFY COLUMN `arr.n` Array(Nullable(String));
KILL MUTATION WHERE database = currentDatabase() AND table = 't_killed_modify_nested'
    AND command LIKE '%MODIFY COLUMN%' FORMAT Null;

-- The kill has to win the race with the mutation, otherwise the part is rewritten and nothing below
-- is under test.
SELECT 'mutations left after the kill', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_killed_modify_nested';

SYSTEM START MERGES t_killed_modify_nested;
ALTER TABLE t_killed_modify_nested DROP COLUMN `arr.s` SETTINGS alter_sync = 2, mutations_sync = 2;

-- `data_version > 1` proves this row is the part the DROP COLUMN mutation produced, not the original.
SELECT 'part shape', part_type, part_storage_type, data_version > 1 FROM system.parts
WHERE database = currentDatabase() AND table = 't_killed_modify_nested' AND active;

SELECT 'part column type', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_killed_modify_nested' AND active AND column = 'arr.n';

-- A part cloned unchanged by the mutation would keep `arr.s` and satisfy every assertion above, so
-- check the dropped member is gone: the column list under test is the one the mutation produced.
SELECT 'dropped column in part', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_killed_modify_nested' AND active AND column = 'arr.s';

SELECT 'metadata column type', type FROM system.columns
WHERE database = currentDatabase() AND table = 't_killed_modify_nested' AND name = 'arr.n';

SELECT 'data', `arr.n` FROM t_killed_modify_nested;

DROP TABLE t_killed_modify_nested;
