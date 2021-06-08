DROP DATABASE IF EXISTS db;
DROP DATABASE IF EXISTS db1;
DROP DATABASE IF EXISTS db2;
DROP DATABASE IF EXISTS db3;

CREATE DATABASE db;
CREATE DATABASE db1;
CREATE DATABASE db2;
CREATE DATABASE db3;

CREATE TABLE db.t   (n Int8) ENGINE=MergeTree ORDER BY n;
CREATE TABLE db1.t1 (n Int8) ENGINE=MergeTree ORDER BY n;
CREATE TABLE db2.t2 (n Int8) ENGINE=MergeTree ORDER BY n;
CREATE TABLE db3.t3 (n Int8) ENGINE=MergeTree ORDER BY n;

INSERT INTO db.t   SELECT * FROM numbers(10);
INSERT INTO db1.t1 SELECT * FROM numbers(10);
INSERT INTO db2.t2 SELECT * FROM numbers(10);
INSERT INTO db3.t3 SELECT * FROM numbers(10);

SELECT 'CREATE TABLE t_merge as db.t ENGINE=Merge(^db, ^t)';
CREATE TABLE db.t_merge as db.t ENGINE=Merge('^db', '^t');

SELECT 'SELECT _database, _table, n FROM db.t_merge ORDER BY _database, _table, n';
SELECT _database, _table, n FROM db.t_merge ORDER BY _database, _table, n;

SELECT 'SELECT _database, _table, n FROM merge(^db, ^t) ORDER BY _database, _table, n';
SELECT _database, _table, n FROM merge('^db', '^t') ORDER BY _database, _table, n;

USE db1;

-- evaluated value of expression should not be treat as repr
SELECT 'CREATE TABLE t_merge_1 as db.t ENGINE=Merge(currentDatabase(), ^t)';
CREATE TABLE db.t_merge_1 as db.t ENGINE=Merge(currentDatabase(), '^t');

SELECT 'SELECT _database, _table, n FROM db.t_merge_1 ORDER BY _database, _table, n';
SELECT _database, _table, n FROM db.t_merge_1 ORDER BY _database, _table, n;

-- evaluated value of expression should not be treat as repr
SELECT 'SELECT _database, _table, n FROM merge(currentDatabase(), ^t) ORDER BY _database, _table, n';
SELECT _database, _table, n FROM merge(currentDatabase(), '^t') ORDER BY _database, _table, n;

DROP DATABASE db;
DROP DATABASE db1;
DROP DATABASE db2;
DROP DATABASE db3;
