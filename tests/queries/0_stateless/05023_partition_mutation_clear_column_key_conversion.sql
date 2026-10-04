-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-shared-catalog
--
-- `no-replicated-database` / `no-shared-merge-tree` / `no-shared-catalog`: the test inspects the
-- raw mutation znode of a `ReplicatedMergeTree` table at a known ZooKeeper path, and
-- `SYSTEM STOP MERGES` must hold on the only replica that could execute the mutation.
--
-- `CLEAR COLUMN` is created by `StorageReplicatedMergeTree::alter` rather than
-- `StorageReplicatedMergeTree::mutate`. Its entry must therefore also be scoped with
-- `IN PARTITION ID`, so that a key-safe partition key type change (e.g. `Enum8 -> Int8`) does
-- not make the original partition literal undecodable. Executing such an entry across the type
-- change is covered by `04702` and `04847`; a `CLEAR COLUMN` cannot be kept pending across it,
-- because the replication queue refuses to apply the next `ALTER_METADATA` before the data
-- mutation of the previous alter is done.

DROP TABLE IF EXISTS t_05023 SYNC;

CREATE TABLE t_05023 (p Enum8('a' = 1, 'b' = 2), n Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05023', '1')
PARTITION BY p ORDER BY tuple();

INSERT INTO t_05023 VALUES ('a', 1), ('b', 2);

SYSTEM STOP MERGES t_05023;

-- `alter_sync = 0`: merges and mutations are stopped, so the default would wait forever.
ALTER TABLE t_05023 CLEAR COLUMN n IN PARTITION 'a' SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_05023 PULL;

SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05023' AND NOT is_done;

SELECT count(), countIf(value LIKE '%IN PARTITION ID%') FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t_05023/mutations';

SYSTEM START MERGES t_05023;

-- Barrier: waits for the pending `CLEAR COLUMN` as well.
ALTER TABLE t_05023 UPDATE n = n WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_05023 ORDER BY p, n;

DROP TABLE t_05023 SYNC;
