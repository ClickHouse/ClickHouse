DROP TABLE IF EXISTS multidimensional;
CREATE TABLE multidimensional (x UInt64, arr Array(Array(String))) ENGINE = MergeTree ORDER BY x;

INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []]);
SELECT * FROM multidimensional;

ALTER TABLE multidimensional ADD COLUMN t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date));
INSERT INTO multidimensional (t) VALUES (('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;

OPTIMIZE TABLE multidimensional;
SELECT * FROM multidimensional ORDER BY t;

DROP TABLE multidimensional;

CREATE TABLE multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = Memory;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP TABLE multidimensional;

CREATE TABLE multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = TinyLog;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP TABLE multidimensional;

CREATE TABLE multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = StripeLog;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP TABLE multidimensional;

CREATE TABLE multidimensional (x UInt64, arr Array(Array(String)), t Tuple(String, Array(Nullable(String)), Tuple(UInt32, Date))) ENGINE = Log;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP TABLE multidimensional;
