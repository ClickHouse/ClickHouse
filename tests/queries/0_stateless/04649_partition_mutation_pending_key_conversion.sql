-- Regression test: an `IN PARTITION` mutation that is still pending across a safe partition key
-- type change (e.g. `Enum8 -> Int8`) must be loadable *and* executable afterwards. The partition
-- scope of every command is resolved when the mutation is created and persisted with it, so
-- neither loading the mutation nor selecting the parts for it resolves the `IN PARTITION` literal
-- through the new partition key type (which would throw for the literal 'a' read as `Int8`).

DROP TABLE IF EXISTS t_04649;

CREATE TABLE t_04649 (p Enum8('a' = 1, 'b' = 2), n Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple();

INSERT INTO t_04649 VALUES ('a', 1), ('b', 2);

-- Keep the mutation pending: it must survive the restart unfinished and be executed only after it.
SYSTEM STOP MERGES t_04649;

ALTER TABLE t_04649 UPDATE n = n + 100 IN PARTITION 'a' WHERE 1;

-- A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
-- on-disk partition id, but re-parsing the literal 'a' as `Int8` would throw.
ALTER TABLE t_04649 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;

-- Simulate a restart so that the mutation file is read back by `loadMutations`.
DETACH TABLE t_04649;
ATTACH TABLE t_04649;

SYSTEM START MERGES t_04649;

-- The pending mutation is executed after the restart and affects only the partition it was scoped to.
ALTER TABLE t_04649 UPDATE n = n IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_04649 ORDER BY p, n;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_04649' AND NOT is_done;

DROP TABLE t_04649;
