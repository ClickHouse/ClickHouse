-- Tags: no-replicated-database
-- Tag no-replicated-database: combines different alter segment kinds in one query.
--
-- Covers two QUERY_IS_PROHIBITED branches in
--   src/Interpreters/InterpreterAlterQuery.cpp `validateSegmentsCombination`:
--   * partition_commands_count != 0 && execute_commands_count != 0
--     ("Partition and Execute commands can not be used together")
--   * partition_commands_segments_count != 1
--     ("Partition commands must be sequential in alter query")

DROP TABLE IF EXISTS t_alter_seg_validation;

CREATE TABLE t_alter_seg_validation (i Int32, s String)
ENGINE = MergeTree ORDER BY i PARTITION BY i;

INSERT INTO t_alter_seg_validation VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Partition + Execute in the same ALTER → forbidden.
ALTER TABLE t_alter_seg_validation
    DROP PARTITION 1,
    EXECUTE expire_snapshots(); -- { serverError QUERY_IS_PROHIBITED }

-- Two PartitionCommands segments separated by an AlterCommands segment → forbidden.
ALTER TABLE t_alter_seg_validation
    DROP PARTITION 1,
    ADD COLUMN extra UInt32,
    DROP PARTITION 2; -- { serverError QUERY_IS_PROHIBITED }

-- Two PartitionCommands segments separated by a MutationCommands segment → forbidden.
ALTER TABLE t_alter_seg_validation
    DROP PARTITION 1,
    UPDATE s = 'x' WHERE i = 3,
    DROP PARTITION 2; -- { serverError QUERY_IS_PROHIBITED }

-- None of the rejected ALTERs should have taken effect: 3 rows in 3 partitions, no `extra` column.
SELECT count(), uniqExact(_partition_id) FROM t_alter_seg_validation;
SELECT name FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_seg_validation'
ORDER BY name;

-- Sanity: a single PartitionCommands segment with multiple partition ops is allowed
-- (partition_commands_segments_count == 1 happy path).
ALTER TABLE t_alter_seg_validation DROP PARTITION 1, DROP PARTITION 2;
SELECT count() FROM t_alter_seg_validation;

DROP TABLE t_alter_seg_validation;
