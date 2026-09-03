DROP TABLE IF EXISTS nullable_00457;

CREATE TABLE nullable_00457 (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = Log;

INSERT INTO nullable_00457 SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable_00457 ORDER BY s;
SELECT s FROM nullable_00457 ORDER BY s;
SELECT ns FROM nullable_00457 ORDER BY s;
SELECT narr FROM nullable_00457 ORDER BY s;
SELECT s, narr FROM nullable_00457 ORDER BY s;

INSERT INTO nullable_00457 SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE IF EXISTS nullable_00457;

CREATE TABLE nullable_00457 (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = TinyLog;

INSERT INTO nullable_00457 SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable_00457 ORDER BY s;
SELECT s FROM nullable_00457 ORDER BY s;
SELECT ns FROM nullable_00457 ORDER BY s;
SELECT narr FROM nullable_00457 ORDER BY s;
SELECT s, narr FROM nullable_00457 ORDER BY s;

INSERT INTO nullable_00457 SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE IF EXISTS nullable_00457;

CREATE TABLE nullable_00457 (s String, ns Nullable(String), narr Array(Nullable(UInt64))) ENGINE = StripeLog;

INSERT INTO nullable_00457 SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10;
SELECT * FROM nullable_00457 ORDER BY s;
SELECT s FROM nullable_00457 ORDER BY s;
SELECT ns FROM nullable_00457 ORDER BY s;
SELECT narr FROM nullable_00457 ORDER BY s;
SELECT s, narr FROM nullable_00457 ORDER BY s;

INSERT INTO nullable_00457 SELECT toString(number), number % 3 = 1 ? toString(number) : NULL, arrayMap(x -> x % 2 = 1 ? x : NULL, range(number)) FROM system.numbers LIMIT 10, 10;

DROP TABLE nullable_00457;
