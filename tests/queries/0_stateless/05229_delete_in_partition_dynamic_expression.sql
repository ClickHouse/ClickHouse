-- A partition expression is arbitrary user SQL, so it can name a different partition every time it is
-- evaluated. The partitions of a scoped command are therefore resolved exactly once, when the mutation
-- entry is created, and both the on-the-fly read path and the background materialization read that one
-- resolved set: the rows a pending read reports as deleted are the rows the mutation really deletes.

DROP TABLE IF EXISTS t_delete_in_partition_dynamic;
CREATE TABLE t_delete_in_partition_dynamic (p UInt8, id UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_delete_in_partition_dynamic VALUES (1, 1), (2, 2);

-- Mutations of a plain MergeTree run in the background merge pool, so this keeps the mutation pending.
SYSTEM STOP MERGES t_delete_in_partition_dynamic;
-- `min(p)` is 1 here, so the command is scoped to partition 1.
ALTER TABLE t_delete_in_partition_dynamic DELETE IN PARTITION tuple((SELECT min(p) FROM t_delete_in_partition_dynamic)) WHERE 1 SETTINGS alter_sync = 0;

SELECT 'while the mutation is pending';
SELECT p, id FROM t_delete_in_partition_dynamic ORDER BY id SETTINGS apply_mutations_on_fly = 1;

-- Re-evaluating the expression now would give partition 0 instead, which would let the mutation rewrite a
-- different set of parts than the pending read above has been answering from.
INSERT INTO t_delete_in_partition_dynamic VALUES (0, 3);

SYSTEM START MERGES t_delete_in_partition_dynamic;
ALTER TABLE t_delete_in_partition_dynamic DELETE WHERE 0 SETTINGS mutations_sync = 2;

SELECT 'after it materialized';
SELECT p, id FROM t_delete_in_partition_dynamic ORDER BY id;

DROP TABLE t_delete_in_partition_dynamic;
