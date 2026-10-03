-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the test creates two replicas of one table itself.
-- no-shared-merge-tree: the replicas must have their own sets of parts, and the change is not ported to SharedMergeTree yet.

-- A replica that has not fetched the older version of a key yet must not assign a merge that deletes the newer
-- version by row TTL (#122528): its local parts do not cover the partition. Only `r2` assigns merges.

SET optimize_on_insert = 0;
SET optimize_throw_if_noop = 1;

DROP TABLE IF EXISTS r1 SYNC;
DROP TABLE IF EXISTS r2 SYNC;

CREATE TABLE r1 (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/t_replacing_ttl', 'r1', v) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', merge_with_ttl_timeout = 0, replicated_can_become_leader = 0;

CREATE TABLE r2 (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/t_replacing_ttl', 'r2', v) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', merge_with_ttl_timeout = 0;

SYSTEM STOP FETCHES r2;
INSERT INTO r1 VALUES (1, 1, 0, now() + INTERVAL 1 YEAR);
INSERT INTO r2 VALUES (1, 2, 1, now() - INTERVAL 1 DAY);
SYSTEM SYNC REPLICA r1 LIGHTWEIGHT;

-- `r2` has only the part of the marker, and its queue still has to fetch the older version.
OPTIMIZE TABLE r2 FINAL;
SYSTEM SYNC REPLICA r1;
SELECT 'r1 before fetch', v, d FROM r1 FINAL;

SYSTEM START FETCHES r2;
SYSTEM SYNC REPLICA r2;
OPTIMIZE TABLE r2 FINAL;
SYSTEM SYNC REPLICA r1;
SELECT 'r1 after', count() FROM r1 FINAL;
SELECT 'r2 after', count() FROM r2 FINAL;

DROP TABLE r1 SYNC;
DROP TABLE r2 SYNC;
