-- Tags: replica, no-shared-merge-tree, no-replicated-database
-- Tag no-shared-merge-tree: on `SharedMergeTree` the value 3 waits for active replicas only, so the
--     `ReplicatedMergeTree` fallback asserted here does not hold there.
-- Tag no-replicated-database: two explicit replicas share one ZooKeeper path, and a `Replicated`
--     database rewrites the replica name.

DROP TABLE IF EXISTS r1 SYNC;
DROP TABLE IF EXISTS r2 SYNC;

CREATE TABLE r1 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05055/t', 'r1') ORDER BY k;
CREATE TABLE r2 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05055/t', 'r2') ORDER BY k;

INSERT INTO r1 VALUES (1, 1);
SYSTEM SYNC REPLICA r2;

-- `r2` stops being active but stays registered under `/replicas`, so a wait that covers all replicas
-- cannot be satisfied and reports `UNFINISHED` instead of blocking.
DETACH TABLE r2;

-- Mutations: value 3 waits for all replicas, like value 2.
ALTER TABLE r1 UPDATE v = 2 WHERE 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
ALTER TABLE r1 UPDATE v = 3 WHERE 1 SETTINGS mutations_sync = 3; -- { serverError UNFINISHED }
DELETE FROM r1 WHERE k = 1 SETTINGS lightweight_deletes_sync = 3; -- { serverError UNFINISHED }
-- Value 1 waits for the local replica only, which an inactive replica does not affect.
ALTER TABLE r1 UPDATE v = 4 WHERE 1 SETTINGS mutations_sync = 1;

-- Metadata `ALTER`: value 3 waits for all replicas, like value 2.
ALTER TABLE r1 ADD COLUMN a UInt64 SETTINGS alter_sync = 2, replication_wait_for_inactive_replica_timeout = 0; -- { serverError UNFINISHED }
ALTER TABLE r1 ADD COLUMN b UInt64 SETTINGS alter_sync = 3, replication_wait_for_inactive_replica_timeout = 0; -- { serverError UNFINISHED }
ALTER TABLE r1 ADD COLUMN c UInt64 SETTINGS alter_sync = 1;

ATTACH TABLE r2;
DROP TABLE r1 SYNC;
DROP TABLE r2 SYNC;
