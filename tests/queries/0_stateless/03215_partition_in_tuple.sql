CREATE TABLE t(a DateTime64(3), b FixedString(6))
ENGINE = MergeTree
PARTITION BY toStartOfDay(a)
ORDER BY (a, b)
AS SELECT * from values(
  ('2023-01-01 00:00:00.000', 'fd4c03'),
  ('2023-01-01 00:00:00.000', '123456'));

CREATE TABLE t1(a DateTime64(3), b FixedString(6))
ENGINE = MergeTree
PARTITION BY toStartOfDay(a)
ORDER BY (a, b)
AS SELECT '2023-01-01 00:00:00.000', 'fd4c03';

SELECT * FROM t WHERE tuple(a, b) IN (SELECT tuple(a, b) FROM t1);

SELECT * FROM t WHERE tuple(a, b) NOT IN (SELECT tuple(a, b) FROM t1);

SELECT * FROM t WHERE (a, b) IN (SELECT a, b FROM t1);

SELECT * FROM t WHERE (a, b) NOT IN (SELECT a, b FROM t1);

DROP TABLE t; DROP TABLE t1;

CREATE TABLE t(a DateTime, b FixedString(6))
ENGINE = MergeTree
PARTITION BY toStartOfDay(a)
ORDER BY (a, b)
AS SELECT * from values(
  ('2023-01-01 00:00:00', 'fd4c03'),
  ('2023-01-01 00:00:00', '123456'));

CREATE TABLE t1(a DateTime64, b FixedString(6))
ENGINE = MergeTree
PARTITION BY toStartOfDay(a)
ORDER BY (a, b)
AS SELECT '2023-01-01 00:00:00', 'fd4c03';

SELECT * FROM t WHERE tuple(a, b) IN (SELECT tuple(a, b) FROM t1);

SELECT * FROM t WHERE tuple(a, b) NOT IN (SELECT tuple(a, b) FROM t1);

SELECT * FROM t WHERE (a, b) IN (SELECT a, b FROM t1);

SELECT * FROM t WHERE (a, b) NOT IN (SELECT a, b FROM t1);
