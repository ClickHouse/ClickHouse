-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge

-- Regression test: the `ReplicatedMergeTree` counterpart of a pending multi-partition
-- (`IN PARTITION p1, p2`) mutation surviving a safe partition key type change (e.g.
-- `Enum8 -> Int8`). Every listed partition literal is pinned at creation by rewriting it into
-- `IN PARTITION ID '<id>'`, so the entry read back from ZooKeeper is decoded without the
-- partition key.

DROP TABLE IF EXISTS t_05219 SYNC;

CREATE TABLE t_05219 (p Enum8('a' = 1, 'b' = 2, 'c' = 3), n Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05219', '1')
PARTITION BY p ORDER BY tuple();

INSERT INTO t_05219 VALUES ('a', 1);
INSERT INTO t_05219 VALUES ('b', 2);
INSERT INTO t_05219 VALUES ('c', 3);

-- Keep the mutation pending: it must survive the metadata change and the restart unfinished.
SYSTEM STOP MERGES t_05219;

ALTER TABLE t_05219 UPDATE n = n + 100 IN PARTITION 'a', 'b' WHERE 1;

-- The mutation entry becomes visible in `system.mutations` only after the replica loads it
-- back from ZooKeeper, which normally happens asynchronously; pull it explicitly.
SYSTEM SYNC REPLICA t_05219 PULL;

-- The persisted command is scoped to the resolved partition ids, not to the original literals.
SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05219' AND NOT is_done;

-- A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
-- on-disk partition id, but re-parsing the literals 'a' and 'b' as `Int8` would throw.
ALTER TABLE t_05219 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;

-- Simulate a restart so that the mutation entry is read back from ZooKeeper.
DETACH TABLE t_05219;
ATTACH TABLE t_05219;

SYSTEM START MERGES t_05219;

-- The pending mutation is executed after the restart and affects only the partitions it was scoped to.
ALTER TABLE t_05219 UPDATE n = n IN PARTITION 1, 2 WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_05219 ORDER BY p, n;

-- The part of the unlisted partition was never rewritten by the mutations. The block numbers in
-- the part names depend on insert retries, so only check whether a part carries a mutation version.
SELECT partition_id, length(splitByChar('_', name)) = 5 AS mutated
FROM system.parts WHERE database = currentDatabase() AND table = 't_05219' AND active ORDER BY partition_id;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05219' AND NOT is_done;

DROP TABLE t_05219 SYNC;
