-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree: the affected rows are counted only for `MergeTree` and `ReplicatedMergeTree`.

-- Tests for `affected_rows` in `system.mutations` for `ALTER TABLE ... UPDATE` and `DELETE` applied together on `MergeTree`.
-- The commands are applied in order: an `UPDATE` counts only the rows that are not deleted by the previous commands,
-- and a `DELETE` sees the values written by the previous commands. The counts must not depend on whether
-- mutations are applied to a part together or one by one.
-- To apply several mutations together, all of them are queued with `SYSTEM STOP MERGES`, and then
-- a no-op mutation `DELETE WHERE 0` waits for them (it reports 0).
-- A lightweight `DELETE` is executed as the mutation `UPDATE _row_exists = 0`, so it reports its affected rows too.
-- `system.mutations` returns the rows of a table in the order of the mutations and of the commands in a mutation,
-- so the queries need no `ORDER BY`.

-- 1. `DELETE` and then `UPDATE` in one mutation, the update counts only the rows that are not deleted
SELECT '-- delete then update';
DROP TABLE IF EXISTS t_delete_update;
CREATE TABLE t_delete_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_delete_update (id) SELECT number FROM numbers(10);
ALTER TABLE t_delete_update DELETE WHERE id < 3, UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_delete_update';
DROP TABLE t_delete_update;

-- 2. `UPDATE` and then `DELETE` in one mutation, the delete sees the updated values
SELECT '-- update then delete';
DROP TABLE IF EXISTS t_update_delete;
CREATE TABLE t_update_delete (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_update_delete (id) SELECT number FROM numbers(10);
ALTER TABLE t_update_delete UPDATE v = 1 WHERE id < 5, DELETE WHERE v = 1 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_update_delete';
DROP TABLE t_update_delete;

-- 3. Rows are deleted and updated in several parts
SELECT '-- several parts';
DROP TABLE IF EXISTS t_parts;
CREATE TABLE t_parts (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_parts;
INSERT INTO t_parts (id) SELECT number FROM numbers(0, 3);
INSERT INTO t_parts (id) SELECT number FROM numbers(3, 3);
INSERT INTO t_parts (id) SELECT number FROM numbers(6, 3);
SYSTEM START MERGES t_parts;
ALTER TABLE t_parts DELETE WHERE id % 3 = 0, UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_parts';
DROP TABLE t_parts;

-- 4. `UPDATE`, `DELETE`, `UPDATE` in one mutation: the `DELETE` and the second `UPDATE` are applied in one stage
SELECT '-- shared stage';
DROP TABLE IF EXISTS t_shared_stage;
CREATE TABLE t_shared_stage (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shared_stage (id) SELECT number FROM numbers(10);
ALTER TABLE t_shared_stage UPDATE v = 1 WHERE id < 2, DELETE WHERE id = 3, UPDATE w = 2 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_shared_stage';
DROP TABLE t_shared_stage;

-- 5. The same as 1, but in several mutations applied together
SELECT '-- batch delete update';
DROP TABLE IF EXISTS t_batch_delete_update;
CREATE TABLE t_batch_delete_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_delete_update (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_delete_update;
ALTER TABLE t_batch_delete_update DELETE WHERE id < 3;
ALTER TABLE t_batch_delete_update UPDATE v = 1 WHERE id < 5;
SYSTEM START MERGES t_batch_delete_update;
ALTER TABLE t_batch_delete_update DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_delete_update';
DROP TABLE t_batch_delete_update;

-- 6. The same as 5, but the mutations are applied one by one
SELECT '-- separate delete update';
DROP TABLE IF EXISTS t_one_by_one_delete_update;
CREATE TABLE t_one_by_one_delete_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_one_by_one_delete_update (id) SELECT number FROM numbers(10);
ALTER TABLE t_one_by_one_delete_update DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_one_by_one_delete_update UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_one_by_one_delete_update';
DROP TABLE t_one_by_one_delete_update;

-- 7. The same as 2, but in several mutations applied together
SELECT '-- batch update delete';
DROP TABLE IF EXISTS t_batch_update_delete;
CREATE TABLE t_batch_update_delete (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_update_delete (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_update_delete;
ALTER TABLE t_batch_update_delete UPDATE v = 1 WHERE id < 5;
ALTER TABLE t_batch_update_delete DELETE WHERE v = 1;
SYSTEM START MERGES t_batch_update_delete;
ALTER TABLE t_batch_update_delete DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_update_delete';
DROP TABLE t_batch_update_delete;

-- 8. The same as 4, but the first `UPDATE` is in another mutation applied together
SELECT '-- batch shared stage';
DROP TABLE IF EXISTS t_batch_shared_stage;
CREATE TABLE t_batch_shared_stage (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_shared_stage (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_shared_stage;
ALTER TABLE t_batch_shared_stage UPDATE v = 1 WHERE id < 2;
ALTER TABLE t_batch_shared_stage DELETE WHERE id = 3, UPDATE w = 2 WHERE id < 5;
SYSTEM START MERGES t_batch_shared_stage;
ALTER TABLE t_batch_shared_stage DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_shared_stage';
DROP TABLE t_batch_shared_stage;

-- 9. The rows deleted by a lightweight delete before are not counted by an `UPDATE`
SELECT '-- lightweight then update';
DROP TABLE IF EXISTS t_lightweight_update;
CREATE TABLE t_lightweight_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lightweight_update (id) SELECT number FROM numbers(10);
DELETE FROM t_lightweight_update WHERE id < 4 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
ALTER TABLE t_lightweight_update UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_update';
DROP TABLE t_lightweight_update;

-- 10. The rows deleted by a lightweight delete before are not counted by another lightweight delete
SELECT '-- lightweight twice';
DROP TABLE IF EXISTS t_lightweight_twice;
CREATE TABLE t_lightweight_twice (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lightweight_twice SELECT number FROM numbers(10);
DELETE FROM t_lightweight_twice WHERE id < 3 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
DELETE FROM t_lightweight_twice WHERE id < 5 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_twice';
DROP TABLE t_lightweight_twice;

-- 11. Consecutive lightweight deletes may be coalesced into one command, which counts all their rows for the first one. They are
-- never coalesced in CI, where `tests/config/users.d/limits.yaml` sets `max_bytes_in_set`, so only the totals of the pairs that may
-- be coalesced are checked, which are the same either way:
-- 'a' + 'b' = 5, the `UPDATE` = 4, 'c' + 'd' = 3, the no-op `DELETE WHERE 0` = 0.
SELECT '-- maybe coalesced';
DROP TABLE IF EXISTS t_maybe_coalesced_lightweight;
CREATE TABLE t_maybe_coalesced_lightweight (p UInt64, k String, v UInt64) ENGINE = MergeTree ORDER BY (p, k)
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, enable_row_mask_update_coalescing = 1;
INSERT INTO t_maybe_coalesced_lightweight (p, k) VALUES (1, 'a'), (1, 'a'), (1, 'b'), (1, 'b'), (1, 'b'), (1, 'c'), (1, 'd'), (1, 'd'), (1, 'e');
SYSTEM STOP MERGES t_maybe_coalesced_lightweight;
DELETE FROM t_maybe_coalesced_lightweight WHERE p = 1 AND k IN 'a' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
DELETE FROM t_maybe_coalesced_lightweight WHERE p = 1 AND k IN 'b' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
ALTER TABLE t_maybe_coalesced_lightweight UPDATE v = 1 WHERE 1;
DELETE FROM t_maybe_coalesced_lightweight WHERE p = 1 AND k IN 'c' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
DELETE FROM t_maybe_coalesced_lightweight WHERE p = 1 AND k IN 'd' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
SYSTEM START MERGES t_maybe_coalesced_lightweight;
ALTER TABLE t_maybe_coalesced_lightweight DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT rows[1] + rows[2], rows[3], rows[4] + rows[5], rows[6]
FROM
(
    SELECT groupArray(affected_rows) AS rows
    FROM (SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_maybe_coalesced_lightweight' ORDER BY toUInt64(extract(mutation_id, '\\d+')))
);
DROP TABLE t_maybe_coalesced_lightweight;

-- 12. The same as 9, but the lightweight delete is stored in a patch part instead of the `_row_exists` column of the part
SELECT '-- patch part delete';
DROP TABLE IF EXISTS t_patch_lightweight_update;
CREATE TABLE t_patch_lightweight_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch_lightweight_update (id) SELECT number FROM numbers(10);
DELETE FROM t_patch_lightweight_update WHERE id < 4 SETTINGS lightweight_delete_mode = 'lightweight_update_force';
ALTER TABLE t_patch_lightweight_update UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_patch_lightweight_update';
DROP TABLE t_patch_lightweight_update;

-- 13. The rows deleted by a lightweight delete are not counted by an `ALTER DELETE` applied to the part together with it,
-- the same as when they are applied one by one: the lightweight delete reports 3, and the `ALTER DELETE` reports 2.
SELECT '-- lightweight then delete';
DROP TABLE IF EXISTS t_batch_lightweight_alter_delete;
CREATE TABLE t_batch_lightweight_alter_delete (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_lightweight_alter_delete SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_lightweight_alter_delete;
DELETE FROM t_batch_lightweight_alter_delete WHERE id < 3 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
ALTER TABLE t_batch_lightweight_alter_delete DELETE WHERE id < 5;
SYSTEM START MERGES t_batch_lightweight_alter_delete;
ALTER TABLE t_batch_lightweight_alter_delete DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_lightweight_alter_delete';
DROP TABLE t_batch_lightweight_alter_delete;

-- 14. A known limitation: a constant condition that deletes all rows also stops the counting of the commands before it,
-- because the pipeline stops before reading the part. The `UPDATE` reports 0 instead of 4, and the `DELETE` reports 0 instead of 10.
SELECT '-- constant delete limitation';
DROP TABLE IF EXISTS t_update_constant_delete;
CREATE TABLE t_update_constant_delete (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_update_constant_delete (id) SELECT number FROM numbers(10);
ALTER TABLE t_update_constant_delete UPDATE v = 1 WHERE id < 4, DELETE WHERE 1 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_update_constant_delete';
DROP TABLE t_update_constant_delete;
