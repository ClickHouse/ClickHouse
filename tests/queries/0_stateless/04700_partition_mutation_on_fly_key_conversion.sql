-- Regression test: a pending `IN PARTITION` mutation that survives a safe partition key type
-- change (e.g. `Enum8 -> Int8`) must also be applicable on the fly. `AlterConversions` rebuilds
-- partition-scoped `UPDATE` / lightweight `DELETE` commands for reads with
-- `apply_mutations_on_fly = 1`, and the rebuilt commands must keep the resolved partition scope
-- instead of re-parsing the `IN PARTITION` literal through the new partition key type (which
-- would throw for the literal 'a' read as `Int8`).

DROP TABLE IF EXISTS t_04700;

CREATE TABLE t_04700 (p Enum8('a' = 1, 'b' = 2), n Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple();

INSERT INTO t_04700 VALUES ('a', 1), ('b', 2), ('b', 3);

-- Keep the mutations pending, so reads have to apply them on the fly.
SYSTEM STOP MERGES t_04700;

SET lightweight_deletes_sync = 0;

ALTER TABLE t_04700 UPDATE n = n + 100 IN PARTITION 'a' WHERE 1;
DELETE FROM t_04700 IN PARTITION 'b' WHERE n = 2;

-- A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
-- on-disk partition id, but re-parsing the literal 'a' as `Int8` would throw.
ALTER TABLE t_04700 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;

-- No DETACH/ATTACH here: it would reset the merges blocker and let the pending mutations
-- execute in the background instead of on the fly (loading the persisted scope back from the
-- mutation files is covered by 04649).

-- Both pending mutations are applied on the fly and affect only the partitions they are scoped to.
SELECT p, n FROM t_04700 ORDER BY p, n SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_04700;

-- The pending mutations are executed in the background and produce the same result.
-- The barrier mutation must not be partition-scoped: waiting for a mutation scoped to
-- partition 1 would not wait for the pending lightweight delete in partition 2.
ALTER TABLE t_04700 UPDATE n = n WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_04700 ORDER BY p, n SETTINGS apply_mutations_on_fly = 0;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_04700' AND NOT is_done;

DROP TABLE t_04700;
