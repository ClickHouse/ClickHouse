-- A partition-scoped mutation of plain `MergeTree` must also apply on the fly to the patch parts
-- of that partition. Patch parts live in synthetic `patch-*` partitions, so the scope has to be
-- matched against the original partition id; otherwise the patch part keeps serving the values
-- written by the lightweight update until the mutation is materialized.

DROP TABLE IF EXISTS t_05355;

CREATE TABLE t_05355 (p String, n Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_05355 VALUES ('a', 1), ('b', 2);

SET allow_experimental_lightweight_update = 1;
UPDATE t_05355 SET n = n + 10 WHERE 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_05355' AND active AND startsWith(partition_id, 'patch-');

-- Keep the mutation pending, so reads have to apply it on the fly.
SYSTEM STOP MERGES t_05355;

ALTER TABLE t_05355 CLEAR COLUMN n IN PARTITION 'a' SETTINGS alter_sync = 0;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_05355' AND NOT is_done;

SELECT p, n FROM t_05355 ORDER BY p SETTINGS apply_mutations_on_fly = 1;
SELECT p, n FROM t_05355 ORDER BY p SETTINGS apply_mutations_on_fly = 0;

SYSTEM START MERGES t_05355;
ALTER TABLE t_05355 UPDATE n = n WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_05355 ORDER BY p;

DROP TABLE t_05355;
