DROP DATABASE IF EXISTS test_01109;
SET allow_experimental_database_atomic=1;
CREATE DATABASE test_01109 ENGINE=Atomic;

USE test_01109;

CREATE TABLE t0 ENGINE=MergeTree() ORDER BY tuple() AS SELECT rowNumberInAllBlocks(), * FROM (SELECT toLowCardinality(arrayJoin(['exchange', 'tables'])));
CREATE TABLE t1 ENGINE=Log() AS SELECT * FROM system.tables AS t JOIN system.databases AS d ON t.database=d.name;
CREATE TABLE t2 ENGINE=MergeTree() ORDER BY tuple() AS SELECT rowNumberInAllBlocks() + (SELECT count() FROM t0), * FROM (SELECT arrayJoin(['hello', 'world']));

EXCHANGE TABLES t1 AND t3; -- { serverError 60 }
EXCHANGE TABLES t4 AND t2; -- { serverError 60 }
RENAME TABLE t0 TO t1; -- { serverError 57 }
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
SET default_database_engine='Atomic';
CREATE DATABASE test_01109_other_atomic;
SET default_database_engine='Ordinary';
CREATE DATABASE test_01109_ordinary;

CREATE TABLE test_01109_other_atomic.t3 ENGINE=MergeTree() ORDER BY tuple()
    AS SELECT rowNumberInAllBlocks() + (SELECT max((*,*).1.1) + 1 FROM (SELECT (*,) FROM t1 UNION ALL SELECT (*,) FROM t2)), *
    FROM (SELECT arrayJoin(['another', 'db']));

CREATE TABLE test_01109_ordinary.t4 AS t1;

EXCHANGE TABLES test_01109_other_atomic.t3 AND test_01109_ordinary.t4; -- { serverError 48 }
EXCHANGE TABLES test_01109_ordinary.t4 AND test_01109_other_atomic.t3; -- { serverError 48 }
EXCHANGE TABLES test_01109_ordinary.t4 AND test_01109_ordinary.t4; -- { serverError 48 }

EXCHANGE TABLES t1 AND test_01109_other_atomic.t3;
EXCHANGE TABLES t2 AND t2;
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM test_01109_other_atomic.t3;
SELECT * FROM test_01109_ordinary.t4;

DROP DATABASE test_01109;
DROP DATABASE test_01109_other_atomic;
DROP DATABASE test_01109_ordinary;
