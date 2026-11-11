-- `MOVE PARTITION ... TO TABLE` must be rejected when the *destination* table is read-only via the
-- `table_readonly` MergeTree setting, even when the source table is writable. The command is dispatched
-- through the source table, so the source-side gate in `MergeTreeData::alterPartition` does not fire for
-- a writable source; the destination is checked in `StorageMergeTree::movePartitionToTable`.

DROP TABLE IF EXISTS t_move_src;
DROP TABLE IF EXISTS t_move_dst;

CREATE TABLE t_move_src (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k PARTITION BY k % 4;

CREATE TABLE t_move_dst (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k PARTITION BY k % 4;

INSERT INTO t_move_src SELECT number, number FROM numbers(16);

-- The source is writable, only the destination is read-only.
ALTER TABLE t_move_dst MODIFY SETTING table_readonly = 1;

-- Moving a partition into the read-only destination is rejected and nothing is moved.
ALTER TABLE t_move_src MOVE PARTITION 0 TO TABLE t_move_dst; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

-- Neither table was modified.
SELECT count() FROM t_move_src;
SELECT count() FROM t_move_dst;

-- The setting can always be toggled back, and the move works again afterwards.
ALTER TABLE t_move_dst MODIFY SETTING table_readonly = 0;
ALTER TABLE t_move_src MOVE PARTITION 0 TO TABLE t_move_dst;

SELECT count() FROM t_move_src;
SELECT count() FROM t_move_dst;

DROP TABLE t_move_src;
DROP TABLE t_move_dst;
