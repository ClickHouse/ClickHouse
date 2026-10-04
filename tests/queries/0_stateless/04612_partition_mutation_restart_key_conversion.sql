-- Regression test: a pending/finished `IN PARTITION` mutation must remain loadable after a
-- safe partition key type change (e.g. `Enum8 -> Int8`). The set of affected partitions is
-- persisted in the mutation file, so `loadMutations` no longer re-parses the `IN PARTITION`
-- literal through the new partition key type on restart (which used to throw and block loading).

DROP TABLE IF EXISTS t_04612;

CREATE TABLE t_04612 (p Enum8('a' = 1, 'b' = 2), n Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple();

INSERT INTO t_04612 VALUES ('a', 1), ('b', 2);

-- A partition-scoped mutation whose scope is stored as the partition id of 'a'.
ALTER TABLE t_04612 UPDATE n = n + 100 IN PARTITION 'a' WHERE 1 SETTINGS mutations_sync = 2;

-- A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
-- on-disk partition id, but re-parsing the literal 'a' as `Int8` would throw.
ALTER TABLE t_04612 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;

-- Simulate a restart so that the mutation file is read back by `loadMutations`.
DETACH TABLE t_04612;
ATTACH TABLE t_04612;

-- The table loads and the mutation result is intact.
SELECT p, n FROM t_04612 ORDER BY p, n;

DROP TABLE t_04612;
