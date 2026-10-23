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

-- 2. No row matches, the part is cloned
DROP TABLE IF EXISTS t_no_match;
CREATE TABLE t_no_match (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_no_match SELECT number FROM numbers(10);
ALTER TABLE t_no_match DELETE WHERE id > 100 SETTINGS mutations_sync = 1;
SELECT 'no match', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_no_match' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_no_match;

-- 3. Rows are deleted from several parts
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

-- 4. Only one of several parts has matching rows
DROP TABLE IF EXISTS t_one_part;
CREATE TABLE t_one_part (id UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_one_part;
INSERT INTO t_one_part SELECT number FROM numbers(0, 3);
INSERT INTO t_one_part SELECT number FROM numbers(3, 3);
INSERT INTO t_one_part SELECT number FROM numbers(6, 3);
SYSTEM START MERGES t_one_part;
ALTER TABLE t_one_part DELETE WHERE id < 2 SETTINGS mutations_sync = 1;
SELECT 'one part matches', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_one_part' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_one_part;

-- 5. Several mutations applied together, no row matches both
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

-- 6. Several mutations applied together, the rows of the second one are already deleted by the first one
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

-- 7. The same overlap in the reverse order
DROP TABLE IF EXISTS t_overlap_rev;
CREATE TABLE t_overlap_rev (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_overlap_rev SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_overlap_rev;
ALTER TABLE t_overlap_rev DELETE WHERE id < 2;
ALTER TABLE t_overlap_rev DELETE WHERE id < 4;
SYSTEM START MERGES t_overlap_rev;
ALTER TABLE t_overlap_rev DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'batch overlap reversed', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_overlap_rev' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_overlap_rev;

-- 8. One mutation with two commands: `system.mutations` has a row for each command, a row is counted for the first command that deletes it
DROP TABLE IF EXISTS t_two_commands;
CREATE TABLE t_two_commands (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_two_commands SELECT number FROM numbers(10);
ALTER TABLE t_two_commands DELETE WHERE id < 4, DELETE WHERE id < 2 SETTINGS mutations_sync = 1;
SELECT 'two commands', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_two_commands' ORDER BY command;
DROP TABLE t_two_commands;

-- 9. Three mutations applied together
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

-- 10. The rows are already deleted by an earlier mutation applied separately
DROP TABLE IF EXISTS t_again;
CREATE TABLE t_again (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_again SELECT number FROM numbers(10);
ALTER TABLE t_again DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_again DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT 'already deleted', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_again' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_again;

-- 11. A row where the predicate is NULL is not deleted and not counted
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null VALUES (NULL), (0), (1);
ALTER TABLE t_null DELETE WHERE x = 0 SETTINGS mutations_sync = 1;
SELECT 'null', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_null' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SELECT 'null rows left', count() FROM t_null;
DROP TABLE t_null;

-- 12. Rows deleted by a lightweight delete before are not counted again
DROP TABLE IF EXISTS t_lightweight;
CREATE TABLE t_lightweight (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lightweight SELECT number FROM numbers(10);
DELETE FROM t_lightweight WHERE id < 2;
ALTER TABLE t_lightweight DELETE WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT 'lightweight delete first', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight' AND command LIKE '%id < 5%' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_lightweight;

-- 13. Only rows of the given partition are deleted and counted
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (id UInt64) ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id;
INSERT INTO t_partition SELECT number FROM numbers(10);
ALTER TABLE t_partition DELETE IN PARTITION 0 WHERE id < 6 SETTINGS mutations_sync = 1;
SELECT 'in partition', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_partition' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_partition;

-- 14. The part is read in many blocks
DROP TABLE IF EXISTS t_blocks;
CREATE TABLE t_blocks (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO t_blocks SELECT number FROM numbers(20);
ALTER TABLE t_blocks DELETE WHERE id % 3 = 0 SETTINGS mutations_sync = 1;
SELECT 'many blocks', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_blocks' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_blocks;

-- 15. Wide and compact parts
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

-- 16. Rows inserted after the mutation finished do not change its count
DROP TABLE IF EXISTS t_insert_after;
CREATE TABLE t_insert_after (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_insert_after SELECT number FROM numbers(10);
ALTER TABLE t_insert_after DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
INSERT INTO t_insert_after SELECT number FROM numbers(3);
SELECT 'insert after', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_insert_after' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SELECT 'insert after rows', count() FROM t_insert_after;
DROP TABLE t_insert_after;

-- 17. All rows of the part are deleted by one command: the part is replaced by an empty one without reading it
DROP TABLE IF EXISTS t_all_rows;
CREATE TABLE t_all_rows (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows SELECT number FROM numbers(10);
ALTER TABLE t_all_rows DELETE WHERE id < 100 SETTINGS mutations_sync = 1;
SELECT 'all rows', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_all_rows;

-- 18. All rows of the part are deleted by several mutations applied together
DROP TABLE IF EXISTS t_all_rows_batch;
CREATE TABLE t_all_rows_batch (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows_batch SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_all_rows_batch;
ALTER TABLE t_all_rows_batch DELETE WHERE id < 6;
ALTER TABLE t_all_rows_batch DELETE WHERE id >= 4;
SYSTEM START MERGES t_all_rows_batch;
ALTER TABLE t_all_rows_batch DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'all rows batch', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows_batch' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_all_rows_batch;

-- 19. A known limitation: a constant condition that deletes all rows, applied together with other commands, reports 0.
-- The filter of the stage is folded to a constant false, so the pipeline stops before reading the part and counting its rows.
DROP TABLE IF EXISTS t_constant_batch;
CREATE TABLE t_constant_batch (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_constant_batch SELECT number FROM numbers(10);
ALTER TABLE t_constant_batch DELETE WHERE 1, DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'constant batch', command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_constant_batch';
DROP TABLE t_constant_batch;

-- 20. The same commands as 19 applied separately: the part is replaced by an empty one, and its rows are counted
DROP TABLE IF EXISTS t_constant_separate;
CREATE TABLE t_constant_separate (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_constant_separate SELECT number FROM numbers(10);
ALTER TABLE t_constant_separate DELETE WHERE 1 SETTINGS mutations_sync = 1;
ALTER TABLE t_constant_separate DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT 'constant separate', command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_constant_separate' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_constant_separate;
