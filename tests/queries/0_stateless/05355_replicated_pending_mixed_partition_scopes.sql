-- Tags: zookeeper

-- On `ReplicatedMergeTree` the queue keys a mutation entry by the partitions of all of its commands, so
-- one `ALTER` with a scoped `CLEAR COLUMN c IN PARTITION '1'` and an unscoped sibling (or another scope)
-- put the entry in every partition and used to hand the scoped command to the parts of other partitions
-- while the entry was pending. Read on the fly from the replica that did not run the `ALTER`.

DROP TABLE IF EXISTS t_pending_scopes_r1 SYNC;
DROP TABLE IF EXISTS t_pending_scopes_r2 SYNC;
CREATE TABLE t_pending_scopes_r1 (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pending_scopes', 'r1') PARTITION BY p ORDER BY id;
CREATE TABLE t_pending_scopes_r2 (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pending_scopes', 'r2') PARTITION BY p ORDER BY id;
INSERT INTO t_pending_scopes_r1 VALUES (1, 1, 10, 100), (2, 2, 20, 200);
SYSTEM SYNC REPLICA t_pending_scopes_r2;

-- Mutations cannot be executed while merges are stopped, so the entries stay pending.
SYSTEM STOP MERGES t_pending_scopes_r1;
SYSTEM STOP MERGES t_pending_scopes_r2;

SELECT 'a scoped and an unscoped command in one pending entry';
ALTER TABLE t_pending_scopes_r1 CLEAR COLUMN c IN PARTITION '1', CLEAR COLUMN d SETTINGS alter_sync = 0;
-- Pulls the replication log and loads the new mutation into the queue of the second replica.
SYSTEM SYNC REPLICA t_pending_scopes_r2 PULL;
SELECT p, id, c, d FROM t_pending_scopes_r2 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_pending_scopes_r1;
SYSTEM START MERGES t_pending_scopes_r2;
ALTER TABLE t_pending_scopes_r1 DELETE WHERE 0 SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA t_pending_scopes_r2;
SELECT 'after it materialized';
SELECT p, id, c, d FROM t_pending_scopes_r2 ORDER BY id;

DROP TABLE t_pending_scopes_r1 SYNC;
DROP TABLE t_pending_scopes_r2 SYNC;

DROP TABLE IF EXISTS t_pending_two_scopes_r1 SYNC;
DROP TABLE IF EXISTS t_pending_two_scopes_r2 SYNC;
CREATE TABLE t_pending_two_scopes_r1 (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pending_two_scopes', 'r1') PARTITION BY p ORDER BY id;
CREATE TABLE t_pending_two_scopes_r2 (p UInt8, id UInt64, c UInt64 DEFAULT 777, d UInt64 DEFAULT 888)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pending_two_scopes', 'r2') PARTITION BY p ORDER BY id;
INSERT INTO t_pending_two_scopes_r1 VALUES (1, 1, 10, 100), (2, 2, 20, 200);
SYSTEM SYNC REPLICA t_pending_two_scopes_r2;

SYSTEM STOP MERGES t_pending_two_scopes_r1;
SYSTEM STOP MERGES t_pending_two_scopes_r2;

SELECT 'two scoped commands in one pending entry do not cross-apply';
ALTER TABLE t_pending_two_scopes_r1 CLEAR COLUMN c IN PARTITION '1', CLEAR COLUMN d IN PARTITION '2' SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_pending_two_scopes_r2 PULL;
SELECT p, id, c, d FROM t_pending_two_scopes_r2 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_pending_two_scopes_r1;
SYSTEM START MERGES t_pending_two_scopes_r2;
ALTER TABLE t_pending_two_scopes_r1 DELETE WHERE 0 SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA t_pending_two_scopes_r2;
SELECT 'after it materialized';
SELECT p, id, c, d FROM t_pending_two_scopes_r2 ORDER BY id;

DROP TABLE t_pending_two_scopes_r1 SYNC;
DROP TABLE t_pending_two_scopes_r2 SYNC;
