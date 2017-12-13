DROP TABLE IF EXISTS test.multidimensional;
CREATE TABLE test.multidimensional (x UInt64, arr Array(Array(String))) ENGINE = MergeTree ORDER BY x;

INSERT INTO test.multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []]);
SELECT * FROM test.multidimensional;

ALTER TABLE test.multidimensional ADD COLUMN t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date));
INSERT INTO test.multidimensional (t) VALUES (('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM test.multidimensional ORDER BY t;

OPTIMIZE TABLE test.multidimensional;
SELECT * FROM test.multidimensional ORDER BY t;

DROP TABLE test.multidimensional;

CREATE TABLE test.multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = Memory;
INSERT INTO test.multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM test.multidimensional ORDER BY t;
DROP TABLE test.multidimensional;

CREATE TABLE test.multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = TinyLog;
INSERT INTO test.multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM test.multidimensional ORDER BY t;
DROP TABLE test.multidimensional;

CREATE TABLE test.multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = StripeLog;
INSERT INTO test.multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM test.multidimensional ORDER BY t;
DROP TABLE test.multidimensional;

CREATE TABLE test.multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = Log;
INSERT INTO test.multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM test.multidimensional ORDER BY t;
DROP TABLE test.multidimensional;
