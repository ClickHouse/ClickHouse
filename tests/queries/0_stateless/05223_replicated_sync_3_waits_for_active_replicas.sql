-- Tags: replica, no-shared-merge-tree, no-replicated-database
-- Tag no-shared-merge-tree: two explicit `ReplicatedMergeTree` replicas share one ZooKeeper path.
-- Tag no-replicated-database: a `Replicated` database rewrites the replica name.

DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;

CREATE TABLE r1 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05223/t', 'r1') ORDER BY k;
CREATE TABLE r2 (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05223/t', 'r2') ORDER BY k;

INSERT INTO r1 VALUES (1, 1);
SYSTEM SYNC REPLICA r2;

-- While `r2` is active, value 3 waits for it, so its result is visible without a sync.
ALTER TABLE r1 UPDATE v = 2 WHERE 1 SETTINGS mutations_sync = 3;
SELECT v FROM r2;
ALTER TABLE r1 ADD COLUMN a UInt64 DEFAULT 7 SETTINGS alter_sync = 3;
SELECT a FROM r2;

-- `r2` stops being active but stays registered under `/replicas`.
DETACH TABLE r2;

-- Value 2 cannot wait for the inactive replica, value 3 does not have to.
ALTER TABLE r1 UPDATE v = 3 WHERE 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
ALTER TABLE r1 UPDATE v = 4 WHERE 1 SETTINGS mutations_sync = 3;
DELETE FROM r1 WHERE k = 2 SETTINGS lightweight_deletes_sync = 3;
ALTER TABLE r1 ADD COLUMN b UInt64 SETTINGS alter_sync = 2, replication_wait_for_inactive_replica_timeout = 0; -- { serverError UNFINISHED }
ALTER TABLE r1 ADD COLUMN c UInt64 SETTINGS alter_sync = 3;

SELECT k, v FROM r1;

ATTACH TABLE r2;
DROP TABLE r1;
DROP TABLE r2;
