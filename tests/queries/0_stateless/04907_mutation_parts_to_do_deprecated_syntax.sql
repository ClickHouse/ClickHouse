-- Tags: replica, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: Old syntax is not allowed
-- Tag no-shared-merge-tree: Old syntax is not allowed

SET allow_deprecated_syntax_for_merge_tree = 1;

DROP TABLE IF EXISTS v0_r1 SYNC;
DROP TABLE IF EXISTS v0_r2 SYNC;

CREATE TABLE v0_r1 (d Date, k UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04907', 'r1', d, k, 8192);
CREATE TABLE v0_r2 (d Date, k UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04907', 'r2', d, k, 8192);

INSERT INTO v0_r2 VALUES ('2015-01-01', 1, 1);
SYSTEM SYNC REPLICA v0_r1;

-- Freeze r2 so that the queue entry produced by the next INSERT stays pending and nothing
-- can make progress while the mutation bookkeeping is inspected.
SYSTEM STOP FETCHES v0_r2;
SYSTEM STOP MERGES v0_r2;

INSERT INTO v0_r1 VALUES ('2015-01-01', 2, 2);
SYSTEM SYNC REPLICA v0_r2 PULL;

-- An entry for a part r2 already holds can still be queued here; it gets skipped, not fetched.
SELECT 'queue entries pending on r2', count() FROM system.replication_queue
    WHERE database = currentDatabase() AND replica_name = 'r2' AND type = 'GET_PART'
        AND new_part_name NOT IN (
            SELECT name FROM system.parts
                WHERE database = currentDatabase() AND table = 'v0_r2' AND active);

ALTER TABLE v0_r1 UPDATE v = 100 WHERE 1 SETTINGS mutations_sync = 0;
SYSTEM SYNC REPLICA v0_r2 PULL;

-- Both the part r2 already has and the part its pending queue entry will produce are older than
-- the mutation, so the mutation cannot be done on r2 while r2 is frozen.
SELECT 'r2 mutation done', max(is_done), 'parts to do', max(parts_to_do) FROM system.mutations
    WHERE database = currentDatabase() AND table = 'v0_r2';

SELECT 'r2 rows before mutation applies', groupArray(v) FROM (SELECT v FROM v0_r2 ORDER BY k);

SYSTEM START FETCHES v0_r2;
SYSTEM START MERGES v0_r2;
SYSTEM SYNC REPLICA v0_r2;

ALTER TABLE v0_r1 UPDATE v = 100 WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'r1 rows', groupArray(v) FROM (SELECT v FROM v0_r1 ORDER BY k);
SELECT 'r2 rows', groupArray(v) FROM (SELECT v FROM v0_r2 ORDER BY k);

DROP TABLE v0_r1 SYNC;
DROP TABLE v0_r2 SYNC;
