-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the affected rows are counted only for `MergeTree` and `ReplicatedMergeTree`

-- Tests for `affected_rows` in `system.mutations` for `ALTER TABLE ... UPDATE` on `MergeTree`.
-- The affected rows of an `UPDATE` command are the rows that match its condition, even if the new value equals the old one.
-- Unlike `DELETE`, a row can be counted by several `UPDATE` commands.
-- To apply several mutations together, all of them are queued with `SYSTEM STOP MERGES`, and then
-- a no-op mutation `UPDATE v = v WHERE 0` waits for them (it reports 0). It is an `UPDATE` and not a `DELETE`,
-- so that the mutations are applied without rewriting all columns, as without it.
-- The rows of `system.mutations` are not sorted: for a table, they are returned in the order of mutations and commands.

-- 1. One mutation
DROP TABLE IF EXISTS t_basic;
CREATE TABLE t_basic (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_basic (id) SELECT number FROM numbers(10);
ALTER TABLE t_basic UPDATE v = 1 WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT 'basic', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_basic';
DROP TABLE t_basic;

-- 2. No row matches, the part is cloned
DROP TABLE IF EXISTS t_no_match;
CREATE TABLE t_no_match (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_no_match (id) SELECT number FROM numbers(10);
ALTER TABLE t_no_match UPDATE v = 1 WHERE id > 100 SETTINGS mutations_sync = 1;
SELECT 'no match', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_no_match';
DROP TABLE t_no_match;

-- 3. A command updates several columns, the rows are counted once
DROP TABLE IF EXISTS t_columns;
CREATE TABLE t_columns (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_columns (id) SELECT number FROM numbers(10);
ALTER TABLE t_columns UPDATE v = 1, w = 2 WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT 'several columns', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_columns';
DROP TABLE t_columns;

-- 4. The matching rows are counted even if their values do not change
DROP TABLE IF EXISTS t_unchanged;
CREATE TABLE t_unchanged (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_unchanged (id) SELECT number FROM numbers(10);
ALTER TABLE t_unchanged UPDATE v = v WHERE id < 4 SETTINGS mutations_sync = 1;
SELECT 'value unchanged', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_unchanged';
DROP TABLE t_unchanged;

-- 5. The rows with the condition NULL are not updated and not counted
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (id UInt64, x Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_null (id, x) SELECT number, if(number IN (1, 3), NULL, number) FROM numbers(10);
ALTER TABLE t_null UPDATE v = 1 WHERE x < 5 SETTINGS mutations_sync = 1;
SELECT 'null', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_null';
DROP TABLE t_null;

-- 6. Only rows of the given partition are updated and counted
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (id UInt64, v UInt64) ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id;
INSERT INTO t_partition (id) SELECT number FROM numbers(10);
ALTER TABLE t_partition UPDATE v = 1 IN PARTITION 0 WHERE id < 6 SETTINGS mutations_sync = 1;
SELECT 'in partition', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_partition';
DROP TABLE t_partition;

-- 7. Rows are updated in several parts
DROP TABLE IF EXISTS t_parts;
CREATE TABLE t_parts (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_parts;
INSERT INTO t_parts (id) SELECT number FROM numbers(0, 3);
INSERT INTO t_parts (id) SELECT number FROM numbers(3, 3);
INSERT INTO t_parts (id) SELECT number FROM numbers(6, 3);
SYSTEM START MERGES t_parts;
ALTER TABLE t_parts UPDATE v = 1 WHERE id % 3 = 0 SETTINGS mutations_sync = 1;
SELECT 'several parts', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_parts';
DROP TABLE t_parts;

-- 8. The part is read in many blocks
DROP TABLE IF EXISTS t_blocks;
CREATE TABLE t_blocks (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO t_blocks (id) SELECT number FROM numbers(10);
ALTER TABLE t_blocks UPDATE v = 1 WHERE id < 7 SETTINGS mutations_sync = 1;
SELECT 'many blocks', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_blocks';
DROP TABLE t_blocks;

-- 9. The same update twice, the second one matches the same rows again
DROP TABLE IF EXISTS t_twice;
CREATE TABLE t_twice (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_twice (id) SELECT number FROM numbers(10);
ALTER TABLE t_twice UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
ALTER TABLE t_twice UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'same update twice', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_twice';
DROP TABLE t_twice;

-- 10. Two commands in one mutation, a row is counted for both of them
DROP TABLE IF EXISTS t_two_commands;
CREATE TABLE t_two_commands (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_two_commands (id) SELECT number FROM numbers(10);
ALTER TABLE t_two_commands UPDATE v = 1 WHERE id < 3, UPDATE w = 1 WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'two commands', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_two_commands';
DROP TABLE t_two_commands;

-- 11. Several mutations applied together
DROP TABLE IF EXISTS t_batch;
CREATE TABLE t_batch (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch;
ALTER TABLE t_batch UPDATE v = 1 WHERE id < 3;
ALTER TABLE t_batch UPDATE w = 1 WHERE id < 5;
SYSTEM START MERGES t_batch;
ALTER TABLE t_batch UPDATE v = v WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch';
DROP TABLE t_batch;

-- 12. Several mutations applied together, the condition of the second one depends on the values written by the first one
DROP TABLE IF EXISTS t_batch_depends;
CREATE TABLE t_batch_depends (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_batch_depends (id) SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_batch_depends;
ALTER TABLE t_batch_depends UPDATE v = 1 WHERE id < 3;
ALTER TABLE t_batch_depends UPDATE w = 1 WHERE v = 1;
SYSTEM START MERGES t_batch_depends;
ALTER TABLE t_batch_depends UPDATE v = v WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch sees earlier update', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_depends';
DROP TABLE t_batch_depends;

-- 13. The same as 12, but the mutations are applied one by one
DROP TABLE IF EXISTS t_one_by_one_depends;
CREATE TABLE t_one_by_one_depends (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_one_by_one_depends (id) SELECT number FROM numbers(10);
ALTER TABLE t_one_by_one_depends UPDATE v = 1 WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_one_by_one_depends UPDATE w = 1 WHERE v = 1 SETTINGS mutations_sync = 1;
SELECT 'one by one sees earlier update', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_one_by_one_depends';
DROP TABLE t_one_by_one_depends;

-- 14. The update of a column recalculates a `MATERIALIZED` column in additional stages, they are not counted
DROP TABLE IF EXISTS t_materialized;
CREATE TABLE t_materialized (id UInt64, v UInt64, m UInt64 MATERIALIZED v * 2) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialized (id) SELECT number FROM numbers(10);
ALTER TABLE t_materialized UPDATE v = 1 WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT 'materialized column', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_materialized';
DROP TABLE t_materialized;
