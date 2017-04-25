DROP TABLE IF EXISTS test.nullable;

CREATE TABLE test.nullable (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = Log;

INSERT INTO test.nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM test.nullable ORDER BY s;
SELECT s FROM test.nullable ORDER BY s;
SELECT ns FROM test.nullable ORDER BY s;
SELECT narr FROM test.nullable ORDER BY s;
SELECT s, narr FROM test.nullable ORDER BY s;

INSERT INTO test.nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE IF EXISTS test.nullable;

CREATE TABLE test.nullable (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = TinyLog;

INSERT INTO test.nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM test.nullable ORDER BY s;
SELECT s FROM test.nullable ORDER BY s;
SELECT ns FROM test.nullable ORDER BY s;
SELECT narr FROM test.nullable ORDER BY s;
SELECT s, narr FROM test.nullable ORDER BY s;

INSERT INTO test.nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE IF EXISTS test.nullable;

CREATE TABLE test.nullable (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = StripeLog;

INSERT INTO test.nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM test.nullable ORDER BY s;
SELECT s FROM test.nullable ORDER BY s;
SELECT ns FROM test.nullable ORDER BY s;
SELECT narr FROM test.nullable ORDER BY s;
SELECT s, narr FROM test.nullable ORDER BY s;

INSERT INTO test.nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE test.nullable;
