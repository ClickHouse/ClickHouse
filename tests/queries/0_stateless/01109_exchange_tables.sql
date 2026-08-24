-- Tags: no-parallel
SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS test_01109;
CREATE DATABASE test_01109 ENGINE=Atomic;

USE test_01109;

CREATE TABLE t0 ENGINE=MergeTree() ORDER BY tuple() AS SELECT rowNumberInAllBlocks(), * FROM (SELECT toLowCardinality(arrayJoin(['exchange', 'tables'])));
-- NOTE: database = currentDatabase() is not mandatory
CREATE TABLE t1 ENGINE=Log() AS SELECT * FROM system.tables AS t JOIN system.databases AS d ON t.database=d.name;
CREATE TABLE t2 ENGINE=MergeTree() ORDER BY tuple() AS SELECT rowNumberInAllBlocks() + (SELECT count() FROM t0), * FROM (SELECT arrayJoin(['hello', 'world']));

EXCHANGE TABLES t1 AND t3; -- { serverError UNKNOWN_TABLE }
EXCHANGE TABLES t4 AND t2; -- { serverError UNKNOWN_TABLE }
RENAME TABLE t0 TO t1; -- { serverError TABLE_ALREADY_EXISTS }
DROP TABLE t1;
RENAME TABLE t0 TO t1;
SELECT * FROM t1;
SELECT * FROM t2;

EXCHANGE TABLES t1 AND t2;
SELECT * FROM t1;
SELECT * FROM t2;

RENAME TABLE t1 TO t1tmp, t2 TO t2tmp;
RENAME TABLE t1tmp TO t2, t2tmp TO t1;
SELECT * FROM t1;
SELECT * FROM t2;

DROP DATABASE IF EXISTS test_01109_other_atomic;
DROP DATABASE IF EXISTS test_01109_ordinary;
CREATE DATABASE test_01109_other_atomic;
set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE test_01109_ordinary ENGINE=Ordinary;

CREATE TABLE test_01109_other_atomic.t3 ENGINE=MergeTree() ORDER BY tuple()
    AS SELECT rowNumberInAllBlocks() + (SELECT max((*,*).1.1) + 1 FROM (SELECT (*,) FROM t1 UNION ALL SELECT (*,) FROM t2)), *
    FROM (SELECT arrayJoin(['another', 'db']));

CREATE TABLE test_01109_ordinary.t4 AS t1;

EXCHANGE TABLES test_01109_other_atomic.t3 AND test_01109_ordinary.t4; -- { serverError NOT_IMPLEMENTED }
EXCHANGE TABLES test_01109_ordinary.t4 AND test_01109_other_atomic.t3; -- { serverError NOT_IMPLEMENTED }
EXCHANGE TABLES test_01109_ordinary.t4 AND test_01109_ordinary.t4; -- { serverError NOT_IMPLEMENTED }

EXCHANGE TABLES t1 AND test_01109_other_atomic.t3;
EXCHANGE TABLES t2 AND t2;
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM test_01109_other_atomic.t3;
SELECT * FROM test_01109_ordinary.t4;

DROP DATABASE IF EXISTS test_01109_rename_exists;
CREATE DATABASE test_01109_rename_exists ENGINE=Atomic;
USE test_01109_rename_exists;
CREATE TABLE t0 ENGINE=Log() AS SELECT * FROM system.numbers limit 2;
RENAME TABLE t0_tmp TO t1; -- { serverError UNKNOWN_TABLE }
RENAME TABLE if exists t0_tmp TO t1;
RENAME TABLE if exists t0 TO t1;
SELECT * FROM t1;

DROP DATABASE test_01109;
DROP DATABASE test_01109_other_atomic;
DROP DATABASE test_01109_ordinary;
DROP DATABASE test_01109_rename_exists;
