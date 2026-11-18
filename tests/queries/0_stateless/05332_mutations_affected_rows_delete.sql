-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the affected rows are counted only for `MergeTree` and `ReplicatedMergeTree`

-- Tests for `affected_rows` in `system.mutations` for `ALTER TABLE ... DELETE` on `MergeTree`.
-- A row is attributed to the first mutation (in mutation order) that deletes it, so the counts
-- must not depend on whether mutations are applied to a part together or one by one.
-- To apply several mutations together, all of them are queued with `SYSTEM STOP MERGES`, and then
-- a no-op mutation `DELETE WHERE 0` waits for them (it reports 0).

-- 1. One mutation
SELECT '-- one mutation';
DROP TABLE IF EXISTS t_basic;
CREATE TABLE t_basic (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_basic SELECT number FROM numbers(10);
ALTER TABLE t_basic DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_basic' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_basic;

-- 2. Rows are deleted from several parts
SELECT '-- several parts';
DROP TABLE IF EXISTS t_parts;
CREATE TABLE t_parts (id UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_parts;
INSERT INTO t_parts SELECT number FROM numbers(0, 3);
INSERT INTO t_parts SELECT number FROM numbers(3, 3);
INSERT INTO t_parts SELECT number FROM numbers(6, 3);
SYSTEM START MERGES t_parts;
ALTER TABLE t_parts DELETE WHERE id % 3 = 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_parts' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_parts;

-- 3. Several mutations applied together, no row matches both
SELECT '-- batch disjoint';
DROP TABLE IF EXISTS t_disjoint;
CREATE TABLE t_disjoint (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_disjoint SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_disjoint;
ALTER TABLE t_disjoint DELETE WHERE id < 2;
ALTER TABLE t_disjoint DELETE WHERE id > 7;
SYSTEM START MERGES t_disjoint;
ALTER TABLE t_disjoint DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_disjoint' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_disjoint;

-- 4. Several mutations applied together, the rows of the second one are already deleted by the first one
SELECT '-- batch overlap';
DROP TABLE IF EXISTS t_overlap;
CREATE TABLE t_overlap (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_overlap SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_overlap;
ALTER TABLE t_overlap DELETE WHERE id < 4;
ALTER TABLE t_overlap DELETE WHERE id < 2;
SYSTEM START MERGES t_overlap;
ALTER TABLE t_overlap DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_overlap' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_overlap;

-- 5. One mutation with two commands: `system.mutations` has a row for each command, a row is counted for the first command that deletes it
SELECT '-- two commands';
DROP TABLE IF EXISTS t_two_commands;
CREATE TABLE t_two_commands (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_two_commands SELECT number FROM numbers(10);
ALTER TABLE t_two_commands DELETE WHERE id < 4, DELETE WHERE id < 2 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_two_commands' ORDER BY command;
DROP TABLE t_two_commands;

-- 6. Three mutations applied together
SELECT '-- batch of three';
DROP TABLE IF EXISTS t_three;
CREATE TABLE t_three (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_three SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_three;
ALTER TABLE t_three DELETE WHERE id < 2;
ALTER TABLE t_three DELETE WHERE id < 4;
ALTER TABLE t_three DELETE WHERE id < 1;
SYSTEM START MERGES t_three;
ALTER TABLE t_three DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_three' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_three;

-- 7. The rows are already deleted by an earlier mutation applied separately
SELECT '-- already deleted';
DROP TABLE IF EXISTS t_again;
CREATE TABLE t_again (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_again SELECT number FROM numbers(10);
ALTER TABLE t_again DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_again DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_again' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_again;

-- 8. A row where the predicate is NULL is not deleted and not counted
SELECT '-- null condition';
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null VALUES (NULL), (0), (1);
ALTER TABLE t_null DELETE WHERE x = 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_null' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SELECT 'rows left', count() FROM t_null;
DROP TABLE t_null;

-- 9. Rows deleted by a lightweight delete before are not counted again
SELECT '-- after lightweight delete';
DROP TABLE IF EXISTS t_lightweight;
CREATE TABLE t_lightweight (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lightweight SELECT number FROM numbers(10);
DELETE FROM t_lightweight WHERE id < 2;
ALTER TABLE t_lightweight DELETE WHERE id < 5 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight' AND command LIKE '%id < 5%' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_lightweight;

-- 10. Only rows of the given partition are deleted and counted
SELECT '-- in partition';
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (id UInt64) ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id;
INSERT INTO t_partition SELECT number FROM numbers(10);
ALTER TABLE t_partition DELETE IN PARTITION 0 WHERE id < 6 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_partition' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_partition;

-- 11. The part is read in many blocks
SELECT '-- many blocks';
DROP TABLE IF EXISTS t_blocks;
CREATE TABLE t_blocks (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO t_blocks SELECT number FROM numbers(20);
ALTER TABLE t_blocks DELETE WHERE id % 3 = 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_blocks' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_blocks;

-- 12. Wide and compact parts
SELECT '-- part types';
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_wide (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_compact (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 0;
INSERT INTO t_wide SELECT number FROM numbers(10);
INSERT INTO t_compact SELECT number FROM numbers(10);
SELECT table, part_type FROM system.parts WHERE database = currentDatabase() AND table IN ('t_wide', 't_compact') AND active ORDER BY table;
ALTER TABLE t_wide DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
ALTER TABLE t_compact DELETE WHERE id < 3 SETTINGS mutations_sync = 1;
SELECT table, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table IN ('t_wide', 't_compact') ORDER BY table;
DROP TABLE t_wide;
DROP TABLE t_compact;

-- 13. All rows of the part are deleted by several mutations applied together: the part is replaced by an empty one without reading
-- it. A known limitation: the conditions are not constant, so the rows of the part are not counted, and all mutations report 0
SELECT '-- all rows batch';
DROP TABLE IF EXISTS t_all_rows_batch;
CREATE TABLE t_all_rows_batch (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows_batch SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_all_rows_batch;
ALTER TABLE t_all_rows_batch DELETE WHERE id < 6;
ALTER TABLE t_all_rows_batch DELETE WHERE id >= 4;
SYSTEM START MERGES t_all_rows_batch;
ALTER TABLE t_all_rows_batch DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows_batch' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SYSTEM FLUSH LOGS part_log;
SELECT 'empty part', sum(ProfileEvents['MutationCreatedEmptyParts']) FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_all_rows_batch' AND event_type = 'MutatePart';
DROP TABLE t_all_rows_batch;

-- 14. A constant condition that deletes all rows, applied together with another command: the part is replaced by an empty one,
-- and the rows are counted for the first command that deletes them
SELECT '-- constant batch';
DROP TABLE IF EXISTS t_constant_batch;
CREATE TABLE t_constant_batch (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_constant_batch SELECT number FROM numbers(10);
ALTER TABLE t_constant_batch DELETE WHERE 1, DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_constant_batch';
DROP TABLE t_constant_batch;

-- 15. The same commands as 14 applied separately: the part is replaced by an empty one, and its rows are counted
SELECT '-- constant separate';
DROP TABLE IF EXISTS t_constant_separate;
CREATE TABLE t_constant_separate (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_constant_separate SELECT number FROM numbers(10);
ALTER TABLE t_constant_separate DELETE WHERE 1 SETTINGS mutations_sync = 1;
ALTER TABLE t_constant_separate DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_constant_separate' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_constant_separate;

-- 16. The rows of a part with a lightweight delete are deleted by several commands: the part is mutated, and the rows deleted by
-- the lightweight delete are not counted again
SELECT '-- all rows lightweight';
DROP TABLE IF EXISTS t_all_rows_lightweight;
CREATE TABLE t_all_rows_lightweight (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows_lightweight SELECT number FROM numbers(10);
DELETE FROM t_all_rows_lightweight WHERE id < 3;
ALTER TABLE t_all_rows_lightweight DELETE WHERE id < 6, DELETE WHERE id >= 4 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows_lightweight' ORDER BY toUInt64(extract(mutation_id, '\\d+')), command;
SYSTEM FLUSH LOGS part_log;
SELECT 'empty part', sum(ProfileEvents['MutationCreatedEmptyParts']) FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_all_rows_lightweight' AND event_type = 'MutatePart';
DROP TABLE t_all_rows_lightweight;

-- 17. All rows of a part with a lightweight delete are deleted by several commands: the part is replaced by an empty one without
-- reading it. A known limitation: the rows left by the lightweight delete are not known without reading the part, so the rows of the
-- part are not counted, and the commands report 0
SELECT '-- lightweight by key';
DROP TABLE IF EXISTS t_all_rows_lightweight_key;
CREATE TABLE t_all_rows_lightweight_key (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_all_rows_lightweight_key SELECT number FROM numbers(10);
DELETE FROM t_all_rows_lightweight_key WHERE id < 5;
ALTER TABLE t_all_rows_lightweight_key DELETE WHERE id < 7, DELETE WHERE id >= 0 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_all_rows_lightweight_key' ORDER BY toUInt64(extract(mutation_id, '\\d+')), command;
SYSTEM FLUSH LOGS part_log;
SELECT 'empty part', sum(ProfileEvents['MutationCreatedEmptyParts']) FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_all_rows_lightweight_key' AND event_type = 'MutatePart';
DROP TABLE t_all_rows_lightweight_key;

-- 18. Mutations with the same condition applied together: the rows are counted for the first one, the next one deletes no row first
SELECT '-- same condition';
DROP TABLE IF EXISTS t_same_condition;
CREATE TABLE t_same_condition (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_same_condition SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_same_condition;
ALTER TABLE t_same_condition DELETE WHERE id < 3;
ALTER TABLE t_same_condition DELETE WHERE id < 3;
ALTER TABLE t_same_condition DELETE WHERE id = 5;
SYSTEM START MERGES t_same_condition;
ALTER TABLE t_same_condition DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_same_condition' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_same_condition;

-- 19. Nondeterministic conditions that delete all rows of the part together: the part is replaced by an empty one, and the conditions
-- are not literals, so the rows are not counted (a known limitation), and no count exceeds the rows of the part
SELECT '-- nondeterministic';
DROP TABLE IF EXISTS t_nondeterministic;
CREATE TABLE t_nondeterministic (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nondeterministic VALUES (0), (1);
ALTER TABLE t_nondeterministic DELETE WHERE rand() % 2 = 0, DELETE WHERE rand() % 2 = 0, DELETE WHERE 1 SETTINGS mutations_sync = 1;
SELECT sum(affected_rows), max(affected_rows) <= 2 FROM system.mutations WHERE database = currentDatabase() AND table = 't_nondeterministic';
DROP TABLE t_nondeterministic;

-- 20. A condition with `exists` and another condition delete all rows of the part together: the mutations complete, and the conditions
-- are not literals, so the rows are not counted (a known limitation)
SELECT '-- subquery exists';
DROP TABLE IF EXISTS t_exists;
CREATE TABLE t_exists (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_exists SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_exists;
ALTER TABLE t_exists DELETE WHERE id < 3;
ALTER TABLE t_exists DELETE WHERE exists((SELECT * FROM system.one));
SYSTEM START MERGES t_exists;
ALTER TABLE t_exists DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_exists' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SELECT count() FROM t_exists;
DROP TABLE t_exists;

-- 21. Several conditions on a column out of the keys delete all rows of the part together, also a constant one. A known limitation:
-- not all conditions are constant, so the rows of the part are not counted, and all commands report 0
SELECT '-- non-key conditions';
DROP TABLE IF EXISTS t_non_key;
CREATE TABLE t_non_key (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_non_key SELECT number, number % 10 FROM numbers(20);
SYSTEM STOP MERGES t_non_key;
ALTER TABLE t_non_key DELETE WHERE v = 0;
ALTER TABLE t_non_key DELETE WHERE v = 1;
ALTER TABLE t_non_key DELETE WHERE v = 2;
ALTER TABLE t_non_key DELETE WHERE v = 0;
ALTER TABLE t_non_key DELETE WHERE 1;
SYSTEM START MERGES t_non_key;
ALTER TABLE t_non_key DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_non_key' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
SYSTEM FLUSH LOGS part_log;
SELECT 'empty part', sum(ProfileEvents['MutationCreatedEmptyParts']) FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_non_key' AND event_type = 'MutatePart';
SELECT count() FROM t_non_key;
DROP TABLE t_non_key;

-- 22. A condition without columns that is not a literal, e.g. `materialize(1)`, is not evaluated for counting, so the mutation completes
-- as on master: its rows are counted when it is the only command, and not counted together with another command (a known limitation)
SELECT '-- not a literal';
DROP TABLE IF EXISTS t_not_literal;
CREATE TABLE t_not_literal (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_not_literal SELECT number FROM numbers(10);
ALTER TABLE t_not_literal DELETE WHERE materialize(1) SETTINGS mutations_sync = 1;
INSERT INTO t_not_literal SELECT number FROM numbers(10);
ALTER TABLE t_not_literal DELETE WHERE materialize(1), DELETE WHERE 0 SETTINGS mutations_sync = 1;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_not_literal' ORDER BY toUInt64(extract(mutation_id, '\\d+')), command;
SELECT count() FROM t_not_literal;
DROP TABLE t_not_literal;
