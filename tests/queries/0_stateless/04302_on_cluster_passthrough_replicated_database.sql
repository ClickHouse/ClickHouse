-- Tags: zookeeper, no-replicated-database, no-ordinary-database

-- Issue #106087: ON CLUSTER pass-through for Replicated databases.
-- When the cluster name in ON CLUSTER matches the automatic cluster name of a
-- `Replicated` database (which is the database name itself), the clause should
-- be treated as a no-op, since the database engine already propagates DDL.

DROP DATABASE IF EXISTS rdb_106087;

CREATE DATABASE rdb_106087 ENGINE = Replicated('/clickhouse/test/{database}/04302', 's1', 'r1') FORMAT NULL;

-- CREATE TABLE with ON CLUSTER == database name (already worked before the fix).
CREATE TABLE rdb_106087.t1 ON CLUSTER rdb_106087 (a Int, b Int) ENGINE = ReplicatedMergeTree ORDER BY a FORMAT NULL;

-- ALTER TABLE with ON CLUSTER == database name (already worked before the fix).
ALTER TABLE rdb_106087.t1 ON CLUSTER rdb_106087 ADD COLUMN c Int FORMAT NULL;
SELECT name FROM system.columns WHERE database = 'rdb_106087' AND table = 't1' ORDER BY name;

-- OPTIMIZE TABLE with ON CLUSTER == database name (added by this fix).
-- Two inserts produce two parts. OPTIMIZE FINAL must merge them down to one,
-- which confirms the OPTIMIZE was actually executed (and not silently dropped
-- after `maybeRemoveOnCluster` stripped the cluster clause).
INSERT INTO rdb_106087.t1 (a, b, c) VALUES (1, 1, 1);
INSERT INTO rdb_106087.t1 (a, b, c) VALUES (1, 2, 2);
OPTIMIZE TABLE rdb_106087.t1 ON CLUSTER rdb_106087 FINAL FORMAT NULL;
SELECT 'optimize merged parts', count() FROM system.parts WHERE database = 'rdb_106087' AND table = 't1' AND active;

-- RENAME TABLE with ON CLUSTER == database name (added by this fix).
RENAME TABLE rdb_106087.t1 TO rdb_106087.t1_renamed ON CLUSTER rdb_106087 FORMAT NULL;
SELECT 'after rename', count() FROM rdb_106087.t1_renamed;
SELECT 'rename preserved merged parts', count() FROM system.parts WHERE database = 'rdb_106087' AND table = 't1_renamed' AND active;

-- EXCHANGE TABLES with ON CLUSTER == database name (added by this fix).
CREATE TABLE rdb_106087.s (a Int, b Int, c Int) ENGINE = ReplicatedMergeTree ORDER BY a FORMAT NULL;
INSERT INTO rdb_106087.s (a, b, c) VALUES (2, 2, 2);
EXCHANGE TABLES rdb_106087.t1_renamed AND rdb_106087.s ON CLUSTER rdb_106087 FORMAT NULL;
SELECT 't1_renamed after exchange', * FROM rdb_106087.t1_renamed ORDER BY a, b, c;
SELECT 's after exchange', * FROM rdb_106087.s ORDER BY a, b, c;
SELECT 'rows after exchange in t1_renamed', count() FROM rdb_106087.t1_renamed;
SELECT 'rows after exchange in s', count() FROM rdb_106087.s;

-- DROP TABLE with ON CLUSTER == database name (already worked before the fix).
DROP TABLE rdb_106087.s ON CLUSTER rdb_106087 FORMAT NULL;

-- ON CLUSTER with a non-matching cluster name should still fail with a clear error
-- (the cluster lookup runs through `executeDDLQueryOnCluster`).
CREATE TABLE rdb_106087.bad ON CLUSTER 'does_not_exist_cluster' (a Int) ENGINE = ReplicatedMergeTree ORDER BY a; -- { serverError CLUSTER_DOESNT_EXIST }
RENAME TABLE rdb_106087.t1_renamed TO rdb_106087.t2 ON CLUSTER 'does_not_exist_cluster'; -- { serverError CLUSTER_DOESNT_EXIST }
OPTIMIZE TABLE rdb_106087.t1_renamed ON CLUSTER 'does_not_exist_cluster'; -- { serverError CLUSTER_DOESNT_EXIST }

-- The existing `ignore_on_cluster_for_replicated_database` setting must keep working
-- for the same query types (no regression).
RENAME TABLE rdb_106087.t1_renamed TO rdb_106087.t1_back ON CLUSTER 'does_not_exist_cluster' SETTINGS ignore_on_cluster_for_replicated_database = 1 FORMAT NULL;
OPTIMIZE TABLE rdb_106087.t1_back ON CLUSTER 'does_not_exist_cluster' FINAL SETTINGS ignore_on_cluster_for_replicated_database = 1 FORMAT NULL;
SELECT 'final', count() FROM rdb_106087.t1_back;

DROP DATABASE rdb_106087 SYNC;
