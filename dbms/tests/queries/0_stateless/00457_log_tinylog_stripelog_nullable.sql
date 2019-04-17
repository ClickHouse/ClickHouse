DROP TABLE IF EXISTS nullable;

CREATE TABLE nullable (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = Log;

INSERT INTO nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable ORDER BY s;
SELECT s FROM nullable ORDER BY s;
SELECT ns FROM nullable ORDER BY s;
SELECT narr FROM nullable ORDER BY s;
SELECT s, narr FROM nullable ORDER BY s;

INSERT INTO nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE IF EXISTS nullable;

CREATE TABLE nullable (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = TinyLog;

INSERT INTO nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable ORDER BY s;
SELECT s FROM nullable ORDER BY s;
SELECT ns FROM nullable ORDER BY s;
SELECT narr FROM nullable ORDER BY s;
SELECT s, narr FROM nullable ORDER BY s;

INSERT INTO nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE IF EXISTS nullable;

CREATE TABLE nullable (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = StripeLog;

INSERT INTO nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable ORDER BY s;
SELECT s FROM nullable ORDER BY s;
SELECT ns FROM nullable ORDER BY s;
SELECT narr FROM nullable ORDER BY s;
SELECT s, narr FROM nullable ORDER BY s;

INSERT INTO nullable SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE nullable;
