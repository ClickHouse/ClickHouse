DROP TABLE IF EXISTS test.nested;

CREATE TABLE test.nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = TinyLog;

INSERT INTO test.nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO test.nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM test.nested ORDER BY x;
SELECT x, n.a FROM test.nested ORDER BY x;
SELECT n.a, n.b FROM test.nested ORDER BY n.a;


DROP TABLE IF EXISTS test.nested;

CREATE TABLE test.nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = Log;

INSERT INTO test.nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO test.nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM test.nested ORDER BY x;
SELECT x, n.a FROM test.nested ORDER BY x;
SELECT n.a, n.b FROM test.nested ORDER BY n.a;


DROP TABLE IF EXISTS test.nested;

CREATE TABLE test.nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = StripeLog;

INSERT INTO test.nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO test.nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM test.nested ORDER BY x;
SELECT x, n.a FROM test.nested ORDER BY x;
SELECT n.a, n.b FROM test.nested ORDER BY n.a;


DROP TABLE IF EXISTS test.nested;

CREATE TABLE test.nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = Memory;

INSERT INTO test.nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO test.nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM test.nested ORDER BY x;
SELECT x, n.a FROM test.nested ORDER BY x;
SELECT n.a, n.b FROM test.nested ORDER BY n.a;


DROP TABLE IF EXISTS test.nested;

CREATE TABLE test.nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = MergeTree ORDER BY x;

INSERT INTO test.nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO test.nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM test.nested ORDER BY x;
SELECT x, n.a FROM test.nested ORDER BY x;
SELECT n.a, n.b FROM test.nested ORDER BY n.a;


DROP TABLE test.nested;
