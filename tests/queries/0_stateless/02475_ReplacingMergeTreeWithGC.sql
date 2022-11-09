-- Tags: zookeeper

DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid);
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
select * from test FINAL;

SELECT '== Insert backups ==';
INSERT INTO test (*) VALUES ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1);
select * from test FINAL;

SELECT '== Insert a second batch with overlaping data ==';
INSERT INTO test (*) VALUES ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 1), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0), ('d2', 2, 1), ('d2', 3, 0), ('d3', 2, 1), ('d3', 3, 0);
select * from test FINAL;

DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid);

-- Expect d6 to be version=3 is_deleted=false
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 3, 0);
-- Insert previous version of 'd6' but only v=3 is_deleted=false will remain
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 2, 1);
SELECT '== Only last version remains after OPTIMIZE W/ CLEANUP ==';
OPTIMIZE TABLE test FINAL CLEANUP;
select * from test;

-- insert d6 v=3 is_deleted=true (timestamp more recent so this version should be the one take into acount)
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 3, 1);

SELECT '== OPTIMIZE W/ CLEANUP (remove d6) ==';
OPTIMIZE TABLE test FINAL CLEANUP;
-- No d6 anymore
select * from test;

DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid) SETTINGS clean_deleted_rows='Always';

SELECT '== Test of the SETTINGS clean_deleted_rows as Alaways ==';
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
OPTIMIZE TABLE test FINAL;
-- d6 has to be removed since we set clean_deleted_rows as 'Always'
select * from test;

SELECT '== Test of the SETTINGS clean_deleted_rows as Never ==';
ALTER TABLE test MODIFY SETTING clean_deleted_rows='Never';
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
OPTIMIZE TABLE test FINAL;
-- d6 has NOT to be removed since we set clean_deleted_rows as 'Never'
select * from test;

SELECT '=== Replicated case ===';

DROP TABLE IF EXISTS testReplica1;
DROP TABLE IF EXISTS testReplica2;

CREATE TABLE testReplica1 (uid String, version UInt32, is_deleted UInt8)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/test_02454/', 'r1', version, is_deleted)
    ORDER BY uid
    SETTINGS clean_deleted_rows='Always';

CREATE TABLE testReplica2 (uid String, version UInt32, is_deleted UInt8)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/test_02454/', 'r2', version, is_deleted)
    ORDER BY uid
    SETTINGS clean_deleted_rows='Always';

INSERT INTO testReplica1 (*) VALUES ('d1', 1, 0),('d2', 1, 0),('d3', 1, 0),('d4', 1, 0);
SYSTEM SYNC REPLICA testReplica2;

INSERT INTO testReplica1 (*) VALUES ('d3', 2, 1);
INSERT INTO testReplica1 (*) VALUES ('d1', 2, 1);

SELECT '== Replica 1 ==';
SELECT * FROM testReplica1 FINAL;
SELECT '== Replica 2 ==';
SELECT * FROM testReplica2 FINAL;

SELECT '== OPTIMIZE on replicas ==';
OPTIMIZE TABLE testReplica2 FINAL;
-- Only d3 to d5 remain
SELECT '== Replica 1 ==';
SELECT * FROM testReplica1;
SELECT '== Replica 2 ==';
SELECT * FROM testReplica1;
