-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the affected rows are counted only for `MergeTree` and `ReplicatedMergeTree`

-- Tests for `affected_rows` in `system.mutations` for `ALTER TABLE ... DELETE` on `MergeTree`.
-- A row is attributed to the first mutation (in mutation order) that deletes it, so the counts
-- must not depend on whether mutations are applied to a part together or one by one.
-- To apply several mutations together, all of them are queued with `SYSTEM STOP MERGES`, and then
-- a no-op mutation `DELETE WHERE 0` waits for them (it reports 0).

-- 1. One mutation
DROP TABLE IF EXISTS t_basic;
CREATE TABLE t_basic (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_basic SELECT number FROM numbers(10);
ALTER TABLE t_basic DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT 'basic', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_basic' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_basic;

-- 2. Rows are deleted from several parts
DROP TABLE IF EXISTS t_parts;
CREATE TABLE t_parts (id UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_parts;
INSERT INTO t_parts SELECT number FROM numbers(0, 3);
INSERT INTO t_parts SELECT number FROM numbers(3, 3);
INSERT INTO t_parts SELECT number FROM numbers(6, 3);
SYSTEM START MERGES t_parts;
ALTER TABLE t_parts DELETE WHERE id % 3 = 0 SETTINGS mutations_sync = 1;
SELECT 'several parts', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_parts' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_parts;

-- 3. Several mutations applied together, no row matches both
DROP TABLE IF EXISTS t_disjoint;
CREATE TABLE t_disjoint (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_disjoint SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_disjoint;
ALTER TABLE t_disjoint DELETE WHERE id < 2;
ALTER TABLE t_disjoint DELETE WHERE id > 7;
SYSTEM START MERGES t_disjoint;
ALTER TABLE t_disjoint DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch disjoint', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_disjoint' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_disjoint;

-- 4. Several mutations applied together, the rows of the second one are already deleted by the first one
DROP TABLE IF EXISTS t_overlap;
CREATE TABLE t_overlap (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_overlap SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_overlap;
ALTER TABLE t_overlap DELETE WHERE id < 4;
ALTER TABLE t_overlap DELETE WHERE id < 2;
SYSTEM START MERGES t_overlap;
ALTER TABLE t_overlap DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch overlap', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_overlap' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_overlap;

-- 5. One mutation with two commands: `system.mutations` has a row for each command, a row is counted for the first command that deletes it
DROP TABLE IF EXISTS t_two_commands;
CREATE TABLE t_two_commands (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_two_commands SELECT number FROM numbers(10);
ALTER TABLE t_two_commands DELETE WHERE id < 4, DELETE WHERE id < 2 SETTINGS mutations_sync = 1;
SELECT 'two commands', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_two_commands' ORDER BY command;
DROP TABLE t_two_commands;

-- 6. Three mutations applied together
DROP TABLE IF EXISTS t_three;
CREATE TABLE t_three (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_three SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_three;
ALTER TABLE t_three DELETE WHERE id < 2;
ALTER TABLE t_three DELETE WHERE id < 4;
ALTER TABLE t_three DELETE WHERE id < 1;
SYSTEM START MERGES t_three;
ALTER TABLE t_three DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch of three', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_three' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_three;

-- 7. The rows are already deleted by an earlier mutation applied separately
DROP TABLE IF EXISTS t_again;
CREATE TABLE t_again (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_again SELECT number FROM numbers(10);
ALTER TABLE t_again DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_again DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT 'already deleted', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_again' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_again;

-- 8. A row where the predicate is NULL is not deleted and not counted
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null VALUES (NULL), (0), (1);
ALTER TABLE t_null DELETE WHERE x = 0 SETTINGS mutations_sync = 1;
SELECT 'null', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_null' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SELECT 'null rows left', count() FROM t_null;
DROP TABLE t_null;

-- 9. Rows deleted by a lightweight delete before are not counted again
DROP TABLE IF EXISTS t_lightweight;
CREATE TABLE t_lightweight (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lightweight SELECT number FROM numbers(10);
DELETE FROM t_lightweight WHERE id < 2;
ALTER TABLE t_lightweight DELETE WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'lightweight delete first', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight' AND command LIKE '%id < 5%' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_lightweight;

-- 10. Only rows of the given partition are deleted and counted
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (id UInt64) ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id;
INSERT INTO t_partition SELECT number FROM numbers(10);
ALTER TABLE t_partition DELETE IN PARTITION 0 WHERE id < 6 SETTINGS mutations_sync = 1;
SELECT 'in partition', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_partition' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_partition;

-- 11. The part is read in many blocks
DROP TABLE IF EXISTS t_blocks;
CREATE TABLE t_blocks (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO t_blocks SELECT number FROM numbers(20);
ALTER TABLE t_blocks DELETE WHERE id % 3 = 0 SETTINGS mutations_sync = 1;
SELECT 'many blocks', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_blocks' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_blocks;

-- 12. Wide and compact parts
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_wide (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_compact (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 0;
INSERT INTO t_wide SELECT number FROM numbers(10);
INSERT INTO t_compact SELECT number FROM numbers(10);
SELECT 'part type', table, part_type FROM system.parts WHERE database = currentDatabase() AND table IN ('t_wide', 't_compact') AND active ORDER BY table;
ALTER TABLE t_wide DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_compact DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT 'wide and compact', table, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table IN ('t_wide', 't_compact') ORDER BY table;
DROP TABLE t_wide;
DROP TABLE t_compact;

-- 13. All rows of the part are deleted by several mutations applied together: the part is replaced by an empty one, and the rows
-- are counted for the first mutation that deletes them
DROP TABLE IF EXISTS t_all_rows_batch;
CREATE TABLE t_all_rows_batch (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows_batch SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_all_rows_batch;
ALTER TABLE t_all_rows_batch DELETE WHERE id < 6;
ALTER TABLE t_all_rows_batch DELETE WHERE id >= 4;
SYSTEM START MERGES t_all_rows_batch;
ALTER TABLE t_all_rows_batch DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'all rows batch', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows_batch' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SYSTEM FLUSH LOGS part_log;
SELECT 'all rows batch empty part', sum(ProfileEvents['MutationCreatedEmptyParts']) FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_all_rows_batch' AND event_type = 'MutatePart';
DROP TABLE t_all_rows_batch;

-- 14. A constant condition that deletes all rows, applied together with another command: the part is replaced by an empty one,
-- and the rows are counted for the first command that deletes them
DROP TABLE IF EXISTS t_constant_batch;
CREATE TABLE t_constant_batch (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_constant_batch SELECT number FROM numbers(10);
ALTER TABLE t_constant_batch DELETE WHERE 1, DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'constant batch', command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_constant_batch';
DROP TABLE t_constant_batch;

-- 15. The same commands as 14 applied separately: the part is replaced by an empty one, and its rows are counted
DROP TABLE IF EXISTS t_constant_separate;
CREATE TABLE t_constant_separate (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_constant_separate SELECT number FROM numbers(10);
ALTER TABLE t_constant_separate DELETE WHERE 1 SETTINGS mutations_sync = 1;
ALTER TABLE t_constant_separate DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'constant separate', command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_constant_separate' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_constant_separate;

-- 16. The rows of a part with a lightweight delete are deleted by several commands: the part is mutated, and the rows deleted by
-- the lightweight delete are not counted again
DROP TABLE IF EXISTS t_all_rows_lightweight;
CREATE TABLE t_all_rows_lightweight (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows_lightweight SELECT number FROM numbers(10);
DELETE FROM t_all_rows_lightweight WHERE id < 3;
ALTER TABLE t_all_rows_lightweight DELETE WHERE id < 6, DELETE WHERE id >= 4 SETTINGS mutations_sync = 1;
SELECT 'all rows lightweight', command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows_lightweight' ORDER BY toUInt64(extract(mutation_id, '\\d+')), command;
SYSTEM FLUSH LOGS part_log;
SELECT 'all rows lightweight empty part', sum(ProfileEvents['MutationCreatedEmptyParts']) FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_all_rows_lightweight' AND event_type = 'MutatePart';
DROP TABLE t_all_rows_lightweight;

-- 17. All rows of a part with a lightweight delete are deleted by several commands whose conditions cover the whole part by the
-- primary key: the part is replaced by an empty one, the rows are counted for the first command that deletes them, and the rows
-- deleted by the lightweight delete are not counted again
DROP TABLE IF EXISTS t_all_rows_lightweight_key;
CREATE TABLE t_all_rows_lightweight_key (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows_lightweight_key SELECT number FROM numbers(10);
DELETE FROM t_all_rows_lightweight_key WHERE id < 5;
ALTER TABLE t_all_rows_lightweight_key DELETE WHERE id < 7, DELETE WHERE id >= 0 SETTINGS mutations_sync = 1;
SELECT 'all rows lightweight key', command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows_lightweight_key' ORDER BY toUInt64(extract(mutation_id, '\\d+')), command;
SYSTEM FLUSH LOGS part_log;
SELECT 'all rows lightweight key empty part', sum(ProfileEvents['MutationCreatedEmptyParts']) FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_all_rows_lightweight_key' AND event_type = 'MutatePart';
DROP TABLE t_all_rows_lightweight_key;
