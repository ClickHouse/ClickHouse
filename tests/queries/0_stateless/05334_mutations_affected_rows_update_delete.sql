-- Tests for `affected_rows` in `system.mutations` for `ALTER TABLE ... UPDATE` and `DELETE` applied together on `MergeTree`.
-- The commands are applied in order: an `UPDATE` counts only the rows that are not deleted by the previous commands,
-- and a `DELETE` sees the values written by the previous commands. The counts must not depend on whether
-- mutations are applied to a part together or one by one.
-- To apply several mutations together, all of them are queued with `SYSTEM STOP MERGES`, and then
-- a no-op mutation `DELETE WHERE 0` waits for them (it reports 0).
-- A lightweight `DELETE` is executed as the mutation `UPDATE _row_exists = 0`, so it reports its affected rows too.
-- The rows of `system.mutations` are not sorted: for a table, they are returned in the order of mutations and commands.

-- 1. `DELETE` and then `UPDATE` in one mutation, the update counts only the rows that are not deleted
DROP TABLE IF EXISTS t_delete_update;
CREATE TABLE t_delete_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_delete_update (id) SELECT number FROM numbers(10);
ALTER TABLE t_delete_update DELETE WHERE id < 3, UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'delete then update', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_delete_update';
DROP TABLE t_delete_update;

-- 2. `UPDATE` and then `DELETE` in one mutation, the delete sees the updated values
DROP TABLE IF EXISTS t_update_delete;
CREATE TABLE t_update_delete (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_update_delete (id) SELECT number FROM numbers(10);
ALTER TABLE t_update_delete UPDATE v = 1 WHERE id < 5, DELETE WHERE v = 1 SETTINGS mutations_sync = 1;
SELECT 'update then delete', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_update_delete';
DROP TABLE t_update_delete;

-- 3. Rows are deleted and updated in several parts
DROP TABLE IF EXISTS t_parts;
CREATE TABLE t_parts (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_parts;
INSERT INTO t_parts (id) SELECT number FROM numbers(0, 3);
INSERT INTO t_parts (id) SELECT number FROM numbers(3, 3);
INSERT INTO t_parts (id) SELECT number FROM numbers(6, 3);
SYSTEM START MERGES t_parts;
ALTER TABLE t_parts DELETE WHERE id % 3 = 0, UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'several parts', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_parts';
DROP TABLE t_parts;

-- 4. `UPDATE`, `DELETE`, `UPDATE` in one mutation: the `DELETE` and the second `UPDATE` are applied in one stage
DROP TABLE IF EXISTS t_shared_stage;
CREATE TABLE t_shared_stage (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shared_stage (id) SELECT number FROM numbers(10);
ALTER TABLE t_shared_stage UPDATE v = 1 WHERE id < 2, DELETE WHERE id = 3, UPDATE w = 2 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'shared stage', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_shared_stage';
DROP TABLE t_shared_stage;

-- 5. The same as 1, but in several mutations applied together
DROP TABLE IF EXISTS t_batch_delete_update;
CREATE TABLE t_batch_delete_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_delete_update (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_delete_update;
ALTER TABLE t_batch_delete_update DELETE WHERE id < 3;
ALTER TABLE t_batch_delete_update UPDATE v = 1 WHERE id < 5;
SYSTEM START MERGES t_batch_delete_update;
ALTER TABLE t_batch_delete_update DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch delete then update', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_delete_update';
DROP TABLE t_batch_delete_update;

-- 6. The same as 5, but the mutations are applied one by one
DROP TABLE IF EXISTS t_one_by_one_delete_update;
CREATE TABLE t_one_by_one_delete_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_one_by_one_delete_update (id) SELECT number FROM numbers(10);
ALTER TABLE t_one_by_one_delete_update DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_one_by_one_delete_update UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'one by one delete then update', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_one_by_one_delete_update';
DROP TABLE t_one_by_one_delete_update;

-- 7. The same as 2, but in several mutations applied together
DROP TABLE IF EXISTS t_batch_update_delete;
CREATE TABLE t_batch_update_delete (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_update_delete (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_update_delete;
ALTER TABLE t_batch_update_delete UPDATE v = 1 WHERE id < 5;
ALTER TABLE t_batch_update_delete DELETE WHERE v = 1;
SYSTEM START MERGES t_batch_update_delete;
ALTER TABLE t_batch_update_delete DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch update then delete', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_update_delete';
DROP TABLE t_batch_update_delete;

-- 8. The same as 4, but the first `UPDATE` is in another mutation applied together
DROP TABLE IF EXISTS t_batch_shared_stage;
CREATE TABLE t_batch_shared_stage (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_shared_stage (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_shared_stage;
ALTER TABLE t_batch_shared_stage UPDATE v = 1 WHERE id < 2;
ALTER TABLE t_batch_shared_stage DELETE WHERE id = 3, UPDATE w = 2 WHERE id < 5;
SYSTEM START MERGES t_batch_shared_stage;
ALTER TABLE t_batch_shared_stage DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch shared stage', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_shared_stage';
DROP TABLE t_batch_shared_stage;

-- 9. The rows deleted by a lightweight delete before are not counted by an `UPDATE`
DROP TABLE IF EXISTS t_lightweight_update;
CREATE TABLE t_lightweight_update (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lightweight_update (id) SELECT number FROM numbers(10);
DELETE FROM t_lightweight_update WHERE id < 4 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
ALTER TABLE t_lightweight_update UPDATE v = 1 WHERE 1 SETTINGS mutations_sync = 1;
SELECT 'update after lightweight delete', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_update';
DROP TABLE t_lightweight_update;

-- 10. The rows deleted by a lightweight delete before are not counted by another lightweight delete
DROP TABLE IF EXISTS t_lightweight_twice;
CREATE TABLE t_lightweight_twice (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lightweight_twice SELECT number FROM numbers(10);
DELETE FROM t_lightweight_twice WHERE id < 3 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
DELETE FROM t_lightweight_twice WHERE id < 5 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
SELECT 'lightweight delete twice', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_twice';
DROP TABLE t_lightweight_twice;

-- 11. Consecutive lightweight deletes of the form `p = <UInt64> AND k IN '<String>'` applied to a Wide part together are coalesced
-- into one command (see `enable_row_mask_update_coalescing`). Only such lightweight deletes are coalesced, the `UPDATE` between
-- them is not. The rows deleted by all the coalesced commands are counted for the first one, and the others report 0:
-- 'a' reports 5 (2 rows of 'a' and 3 rows of 'b'), 'b' reports 0, the `UPDATE` reports 4 (the rows not deleted before it),
-- 'c' reports 3 (1 row of 'c' and 2 rows of 'd'), 'd' reports 0, and the no-op `DELETE WHERE 0` reports 0.
DROP TABLE IF EXISTS t_coalesced_lightweight;
CREATE TABLE t_coalesced_lightweight (p UInt64, k String, v UInt64) ENGINE = MergeTree ORDER BY (p, k)
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, enable_row_mask_update_coalescing = 1;
INSERT INTO t_coalesced_lightweight (p, k) VALUES (1, 'a'), (1, 'a'), (1, 'b'), (1, 'b'), (1, 'b'), (1, 'c'), (1, 'd'), (1, 'd'), (1, 'e');
SYSTEM STOP MERGES t_coalesced_lightweight;
DELETE FROM t_coalesced_lightweight WHERE p = 1 AND k IN 'a' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
DELETE FROM t_coalesced_lightweight WHERE p = 1 AND k IN 'b' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
ALTER TABLE t_coalesced_lightweight UPDATE v = 1 WHERE 1;
DELETE FROM t_coalesced_lightweight WHERE p = 1 AND k IN 'c' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
DELETE FROM t_coalesced_lightweight WHERE p = 1 AND k IN 'd' SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
SYSTEM START MERGES t_coalesced_lightweight;
ALTER TABLE t_coalesced_lightweight DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'coalesced lightweight deletes', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_coalesced_lightweight';
DROP TABLE t_coalesced_lightweight;
