-- `CLEAR COLUMN` is created by `StorageReplicatedMergeTree::alter` rather than
-- `StorageReplicatedMergeTree::mutate`. Its pending entry must therefore also use
-- `IN PARTITION ID`, so a safe partition-key type change does not make it unparsable.

DROP TABLE IF EXISTS t_05023;

CREATE TABLE t_05023 (p Enum8('a' = 1, 'b' = 2), n Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05023', '1')
PARTITION BY p ORDER BY tuple();

INSERT INTO t_05023 VALUES ('a', 1), ('b', 2);

SYSTEM STOP MERGES t_05023;

ALTER TABLE t_05023 CLEAR COLUMN n IN PARTITION 'a';
SYSTEM SYNC REPLICA t_05023 PULL;

SELECT command FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05023' AND NOT is_done;

ALTER TABLE t_05023 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;
DETACH TABLE t_05023;
ATTACH TABLE t_05023;

SYSTEM START MERGES t_05023;

ALTER TABLE t_05023 UPDATE n = n IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_05023 ORDER BY p, n;

DROP TABLE t_05023;
