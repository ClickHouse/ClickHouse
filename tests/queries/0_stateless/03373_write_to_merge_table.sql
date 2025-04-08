-- Tags: no-parallel

DROP TABLE IF EXISTS test03373_db.test03373_table_1;
DROP TABLE IF EXISTS test03373_db.test03373_table_2;
DROP TABLE IF EXISTS test03373_db.test03373_table_3;
DROP TABLE IF EXISTS test03373_db.test03373_table_4;
DROP TABLE IF EXISTS test03373_db.test03373_merge_ro;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_1;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_2;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_3;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_auto;
DROP DATABASE IF EXISTS test03373_db;

CREATE DATABASE test03373_db;

CREATE TABLE test03373_db.test03373_table_1 (key UInt32, value UInt32) ENGINE=MergeTree() ORDER BY key;
CREATE TABLE test03373_db.test03373_table_2 (key UInt32, value UInt32) ENGINE=MergeTree() ORDER BY key;

CREATE TABLE test03373_db.test03373_merge_ro (key UInt32, value UInt32) ENGINE=Merge(test03373_db, 'test03373_table_\d+');

CREATE TABLE test03373_db.test03373_merge_wr_1 (key UInt32, value UInt32) ENGINE=Merge(test03373_db, 'test03373_table_\d+', test03373_table_2);
CREATE TABLE test03373_db.test03373_merge_wr_2 (key UInt32, value UInt32) ENGINE=Merge(test03373_db, 'test03373_table_\d+', test03373_db.test03373_table_2);
CREATE TABLE test03373_db.test03373_merge_wr_3 (key UInt32, value UInt32) ENGINE=Merge(REGEXP('test03373_.*'), 'test03373_table_\d+', test03373_db.test03373_table_2);

CREATE TABLE test03373_db.test03373_merge_wr_auto (key UInt32, value UInt32) ENGINE=Merge(test03373_db, 'test03373_table_\d+', auto);

INSERT INTO test03373_db.test03373_table_1 VALUES (1,1);

INSERT INTO test03373_db.test03373_merge_wr_1 VALUES (2,1);
INSERT INTO test03373_db.test03373_merge_wr_2 VALUES (2,2);
INSERT INTO test03373_db.test03373_merge_wr_3 VALUES (2,3);

SELECT * FROM test03373_db.test03373_table_2 ORDER BY key, value;

SELECT * FROM test03373_db.test03373_merge_ro ORDER BY key, value;
SELECT * FROM test03373_db.test03373_merge_wr_1 ORDER BY key, value;
SELECT * FROM test03373_db.test03373_merge_wr_2 ORDER BY key, value;
SELECT * FROM test03373_db.test03373_merge_wr_3 ORDER BY key, value;

SELECT * FROM test03373_db.test03373_merge_wr_auto ORDER BY key, value;

-- insert into test03373_table_2
INSERT INTO test03373_db.test03373_merge_wr_auto VALUES (3,1);
SELECT count() FROM test03373_db.test03373_table_2;
SELECT * FROM test03373_db.test03373_table_2 ORDER BY key, value;

CREATE TABLE test03373_db.test03373_table_4 (key UInt32, value UInt32) ENGINE=MergeTree() ORDER BY key;
-- insert into test03373_table_4
INSERT INTO test03373_db.test03373_merge_wr_auto VALUES (3,2);
SELECT count() FROM test03373_db.test03373_table_2;
SELECT * FROM test03373_db.test03373_table_2 ORDER BY key, value;
SELECT count() FROM test03373_db.test03373_table_4;
SELECT * FROM test03373_db.test03373_table_4 ORDER BY key, value;

CREATE TABLE test03373_db.test03373_table_3 (key UInt32, value UInt32) ENGINE=MergeTree() ORDER BY key;
-- insert into test03373_table_4
INSERT INTO test03373_db.test03373_merge_wr_auto VALUES (3,3);
SELECT count() FROM test03373_db.test03373_table_2;
SELECT * FROM test03373_db.test03373_table_2 ORDER BY key, value;
SELECT count() FROM test03373_db.test03373_table_3;
SELECT * FROM test03373_db.test03373_table_3 ORDER BY key, value;
SELECT count() FROM test03373_db.test03373_table_4;
SELECT * FROM test03373_db.test03373_table_4 ORDER BY key, value;

DROP TABLE IF EXISTS test03373_db.test03373_table_1;
DROP TABLE IF EXISTS test03373_db.test03373_table_2;
DROP TABLE IF EXISTS test03373_db.test03373_table_3;
DROP TABLE IF EXISTS test03373_db.test03373_table_4;
DROP TABLE IF EXISTS test03373_db.test03373_merge_ro;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_1;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_2;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_3;
DROP TABLE IF EXISTS test03373_db.test03373_merge_wr_auto;
DROP DATABASE IF EXISTS test03373_db;
