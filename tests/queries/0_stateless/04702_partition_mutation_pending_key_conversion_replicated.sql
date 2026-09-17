-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge

-- Regression test: the `ReplicatedMergeTree` counterpart of a pending `IN PARTITION` mutation
-- surviving a safe partition key type change (e.g. `Enum8 -> Int8`). The replicated mutation
-- entry persists commands in ZooKeeper only as serialized text, so the partition scope is pinned
-- at creation by rewriting `IN PARTITION <value>` into `IN PARTITION ID '<id>'`; a partition id
-- is decoded without the partition key, unlike the original value literal ('a' read as `Int8`
-- would throw).

DROP TABLE IF EXISTS t_04702;

CREATE TABLE t_04702 (p Enum8('a' = 1, 'b' = 2), n Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_04702', '1')
PARTITION BY p ORDER BY tuple();

INSERT INTO t_04702 VALUES ('a', 1), ('b', 2);

-- Keep the mutation pending: it must survive the metadata change and the restart unfinished.
SYSTEM STOP MERGES t_04702;

ALTER TABLE t_04702 UPDATE n = n + 100 IN PARTITION 'a' WHERE 1;

-- The mutation entry becomes visible in `system.mutations` only after the replica loads it
-- back from ZooKeeper, which normally happens asynchronously; pull it explicitly.
SYSTEM SYNC REPLICA t_04702 PULL;

-- The persisted command is scoped to the resolved partition id, not to the original literal.
SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_04702' AND NOT is_done;

-- A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
-- on-disk partition id, but re-parsing the literal 'a' as `Int8` would throw.
ALTER TABLE t_04702 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;

-- Simulate a restart so that the mutation entry is read back from ZooKeeper.
DETACH TABLE t_04702;
ATTACH TABLE t_04702;

SYSTEM START MERGES t_04702;

-- The pending mutation is executed after the restart and affects only the partition it was scoped to.
ALTER TABLE t_04702 UPDATE n = n IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_04702 ORDER BY p, n;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_04702' AND NOT is_done;

DROP TABLE t_04702;
