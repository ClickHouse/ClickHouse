-- Tags: zookeeper

-- The partitions of a command scoped with `IN PARTITION` are resolved once, when the mutation is
-- created, and the partition expression is replaced in the stored command with the `ID` it resolved to.
-- So a data-dependent expression cannot drift afterwards: not on the on-the-fly read path of an `UPDATE`
-- (which rewrites the command before it is applied), not after the table is reloaded, and not between
-- replicas.

DROP TABLE IF EXISTS t_update_in_partition_pinned;
CREATE TABLE t_update_in_partition_pinned (p UInt8, id UInt64, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_update_in_partition_pinned VALUES (1, 1, 10), (2, 2, 20);

SYSTEM STOP MERGES t_update_in_partition_pinned;
-- `min(p)` is 1 here, so the command is scoped to partition 1.
ALTER TABLE t_update_in_partition_pinned UPDATE v = v + 100 IN PARTITION tuple((SELECT min(p) FROM t_update_in_partition_pinned)) WHERE 1 SETTINGS alter_sync = 0;

SELECT 'stored command';
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_update_in_partition_pinned';

SELECT 'pending update, read on the fly';
SELECT p, id, v FROM t_update_in_partition_pinned ORDER BY id SETTINGS apply_mutations_on_fly = 1;

-- Re-evaluating the expression now would give partition 0.
INSERT INTO t_update_in_partition_pinned VALUES (0, 3, 30);

SELECT 'after the table is reloaded';
DETACH TABLE t_update_in_partition_pinned;
ATTACH TABLE t_update_in_partition_pinned;
SYSTEM STOP MERGES t_update_in_partition_pinned;
SELECT p, id, v FROM t_update_in_partition_pinned ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_update_in_partition_pinned;
ALTER TABLE t_update_in_partition_pinned UPDATE v = v WHERE 0 SETTINGS mutations_sync = 2;

SELECT 'after it materialized';
SELECT p, id, v FROM t_update_in_partition_pinned ORDER BY id;

DROP TABLE t_update_in_partition_pinned;

DROP TABLE IF EXISTS t_update_in_partition_pinned_r1 SYNC;
DROP TABLE IF EXISTS t_update_in_partition_pinned_r2 SYNC;
CREATE TABLE t_update_in_partition_pinned_r1 (p UInt8, id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_update_in_partition_pinned', 'r1') PARTITION BY p ORDER BY id;
CREATE TABLE t_update_in_partition_pinned_r2 (p UInt8, id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_update_in_partition_pinned', 'r2') PARTITION BY p ORDER BY id;
INSERT INTO t_update_in_partition_pinned_r1 VALUES (1, 1, 10), (2, 2, 20);
SYSTEM SYNC REPLICA t_update_in_partition_pinned_r2;

SYSTEM STOP MERGES t_update_in_partition_pinned_r1;
SYSTEM STOP MERGES t_update_in_partition_pinned_r2;
ALTER TABLE t_update_in_partition_pinned_r1 UPDATE v = v + 100 IN PARTITION tuple((SELECT min(p) FROM t_update_in_partition_pinned_r1)) WHERE 1 SETTINGS alter_sync = 0;
INSERT INTO t_update_in_partition_pinned_r1 VALUES (0, 3, 30);
-- Waits for the fetch of the new part, not for the mutation, which cannot run with merges stopped.
SYSTEM SYNC REPLICA t_update_in_partition_pinned_r2 LIGHTWEIGHT;

SELECT 'replicated: stored command';
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_update_in_partition_pinned_r2';

SELECT 'replicated: pending update on the other replica';
SELECT p, id, v FROM t_update_in_partition_pinned_r2 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_update_in_partition_pinned_r1;
SYSTEM START MERGES t_update_in_partition_pinned_r2;
ALTER TABLE t_update_in_partition_pinned_r1 UPDATE v = v WHERE 0 SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA t_update_in_partition_pinned_r2;

SELECT 'replicated: after it materialized';
SELECT p, id, v FROM t_update_in_partition_pinned_r2 ORDER BY id;

DROP TABLE t_update_in_partition_pinned_r1 SYNC;
DROP TABLE t_update_in_partition_pinned_r2 SYNC;
