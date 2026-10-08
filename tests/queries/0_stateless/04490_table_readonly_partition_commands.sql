-- Explicit partition commands must be rejected on a table marked read-only via the `table_readonly`
-- MergeTree setting. These commands are dispatched directly to `MergeTreeData::alterPartition`,
-- bypassing `StorageMergeTree::alter` / `assertNotReadonly`, so they need their own gate.

DROP TABLE IF EXISTS t_readonly_partition_commands;

CREATE TABLE t_readonly_partition_commands (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k PARTITION BY k % 4;

INSERT INTO t_readonly_partition_commands SELECT number, number FROM numbers(16);

ALTER TABLE t_readonly_partition_commands MODIFY SETTING table_readonly = 1;

-- Partition mutations are rejected while the table is read-only.
ALTER TABLE t_readonly_partition_commands DROP PARTITION 0; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
ALTER TABLE t_readonly_partition_commands DETACH PARTITION 1; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
ALTER TABLE t_readonly_partition_commands ATTACH PARTITION 1; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

-- Inserts and mutations are rejected as well (pre-existing behavior of `assertNotReadonly`).
INSERT INTO t_readonly_partition_commands VALUES (100, 100); -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

-- Nothing was modified.
SELECT count() FROM t_readonly_partition_commands;

-- The setting can always be toggled back, and partition commands work again afterwards.
ALTER TABLE t_readonly_partition_commands MODIFY SETTING table_readonly = 0;
ALTER TABLE t_readonly_partition_commands DROP PARTITION 0;
SELECT count() FROM t_readonly_partition_commands;

DROP TABLE t_readonly_partition_commands;
