-- While an `ALTER TABLE ... CLEAR COLUMN c IN PARTITION p` was pending on a plain `MergeTree`, reads of
-- *every* partition answered the column's default: the pending command was selected for a part by
-- mutation version alone, with no partition filtering, and the partition the command names was lost. The
-- values came back once the mutation materialized.

DROP TABLE IF EXISTS t_clear_column_partition;
CREATE TABLE t_clear_column_partition (p UInt8, id UInt64, c UInt64 DEFAULT 777)
ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_clear_column_partition VALUES (1, 1, 10), (2, 2, 20);

-- Mutations of a plain MergeTree run in the background merge pool, so this keeps the mutation pending.
SYSTEM STOP MERGES t_clear_column_partition;
-- `alter_sync = 0` is what lets the ALTER return while its mutation is still pending.
ALTER TABLE t_clear_column_partition CLEAR COLUMN c IN PARTITION '1' SETTINGS alter_sync = 0;

SELECT 'while the mutation is pending';
SELECT p, id, c FROM t_clear_column_partition ORDER BY id;

SYSTEM START MERGES t_clear_column_partition;
ALTER TABLE t_clear_column_partition DELETE WHERE 0 SETTINGS mutations_sync = 2;

SELECT 'after it materialized';
SELECT p, id, c FROM t_clear_column_partition ORDER BY id;

SELECT 'a CLEAR COLUMN of the whole table still applies to every partition while pending';
DROP TABLE IF EXISTS t_clear_column_all;
CREATE TABLE t_clear_column_all (p UInt8, id UInt64, c UInt64 DEFAULT 777)
ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_clear_column_all VALUES (1, 1, 10), (2, 2, 20);
SYSTEM STOP MERGES t_clear_column_all;
ALTER TABLE t_clear_column_all CLEAR COLUMN c SETTINGS alter_sync = 0;
SELECT p, id, c FROM t_clear_column_all ORDER BY id;
SYSTEM START MERGES t_clear_column_all;

SELECT 'and a pending DELETE IN PARTITION applied on the fly keeps its partition';
DROP TABLE IF EXISTS t_delete_in_partition;
CREATE TABLE t_delete_in_partition (p UInt8, id UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_delete_in_partition VALUES (1, 1), (2, 2);
SYSTEM STOP MERGES t_delete_in_partition;
ALTER TABLE t_delete_in_partition DELETE IN PARTITION '1' WHERE 1 SETTINGS alter_sync = 0;
SELECT p, id FROM t_delete_in_partition ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SYSTEM START MERGES t_delete_in_partition;
ALTER TABLE t_delete_in_partition DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT p, id FROM t_delete_in_partition ORDER BY id;

SELECT 'the multi-partition form of DELETE IN PARTITION keeps its partitions too';
-- `DELETE`/`UPDATE` accept several partitions in one command, which the AST carries separately from the
-- single-partition form (`ASTAlterCommand::partitions` instead of `ASTAlterCommand::partition`), so the
-- scope has to be respected there as well. The third partition is untouched by the command and proves
-- the filter in both directions.
DROP TABLE IF EXISTS t_delete_in_partitions;
CREATE TABLE t_delete_in_partitions (p UInt8, id UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_delete_in_partitions VALUES (1, 1), (2, 2), (3, 3);
SYSTEM STOP MERGES t_delete_in_partitions;
ALTER TABLE t_delete_in_partitions DELETE IN PARTITION '1', '2' WHERE 1 SETTINGS alter_sync = 0;
SELECT p, id FROM t_delete_in_partitions ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SYSTEM START MERGES t_delete_in_partitions;
ALTER TABLE t_delete_in_partitions DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT p, id FROM t_delete_in_partitions ORDER BY id;

DROP TABLE t_delete_in_partitions;
DROP TABLE t_delete_in_partition;
DROP TABLE t_clear_column_all;
DROP TABLE t_clear_column_partition;
