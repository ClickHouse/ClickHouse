-- Tags: zookeeper, no-replicated-database, no-ordinary-database
-- no-replicated-database: we explicitly create a replicated database
-- no-ordinary-database: requires Replicated engine

-- Exercises DatabaseReplicated::getConsistentMetadataSnapshotImpl to expose a
-- missing 'throw' before Coordination::Exception::fromPath on max_log_ptr
-- (line ~1950 of DatabaseReplicated.cpp). Each DDL bumps max_log_ptr;
-- SYSTEM SYNC DATABASE REPLICA and system.tables queries force re-reads of
-- metadata snapshots through the affected path. Under sanitizers this gives
-- CI the best chance to detect issues from the silently ignored ZK error.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
    ENGINE = Replicated('/clickhouse/databases/{database}_04090', 'shard1', 'replica1');

USE {CLICKHOUSE_DATABASE_1:Identifier};

SET distributed_ddl_output_mode = 'none';

-- Create several tables so the metadata snapshot has content to iterate.
CREATE TABLE t1_04090 (id UInt32, data String) ENGINE = ReplicatedMergeTree ORDER BY id;
CREATE TABLE t2_04090 (id UInt32, value UInt64) ENGINE = ReplicatedMergeTree ORDER BY id;
CREATE TABLE t3_04090 (id UInt32, ts DateTime DEFAULT now()) ENGINE = ReplicatedMergeTree ORDER BY id;

INSERT INTO t1_04090 VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO t2_04090 VALUES (1, 100), (2, 200);
INSERT INTO t3_04090 (id) VALUES (1), (2);

-- system.tables reads replicated DB metadata via tryGetConsistentMetadataSnapshot.
SELECT '--- tables';
SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name;

-- Row counts confirm data is accessible after metadata snapshot.
SELECT '--- counts';
SELECT 't1_04090', count() FROM t1_04090;
SELECT 't2_04090', count() FROM t2_04090;
SELECT 't3_04090', count() FROM t3_04090;

-- SYSTEM SYNC DATABASE REPLICA exercises getConsistentMetadataSnapshotImpl.
SYSTEM SYNC DATABASE REPLICA {CLICKHOUSE_DATABASE_1:Identifier};

-- ALTER adds a DDL log entry, bumping max_log_ptr and causing the next
-- metadata snapshot to re-read it via tryGet (the buggy path).
ALTER TABLE t1_04090 ADD COLUMN extra String DEFAULT 'x';
SELECT '--- after alter';
SELECT id, data, extra FROM t1_04090 ORDER BY id;

-- Another sync after ALTER to re-exercise the snapshot with new max_log_ptr.
SYSTEM SYNC DATABASE REPLICA {CLICKHOUSE_DATABASE_1:Identifier};

-- system.tables again to force another metadata snapshot read after ALTER.
SELECT '--- tables after alter';
SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name;

-- DROP TABLE also bumps the log pointer then snapshots metadata.
DROP TABLE t3_04090 SYNC;
SELECT '--- after drop';
SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name;

-- Final sync to exercise snapshot after DROP changed max_log_ptr.
SYSTEM SYNC DATABASE REPLICA {CLICKHOUSE_DATABASE_1:Identifier};

-- Cleanup
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
SELECT '--- done';
