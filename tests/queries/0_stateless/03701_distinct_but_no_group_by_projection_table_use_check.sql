-- Tags: no-replicated-database
-- no-replicated-database: explain output differs for replicated database.

SELECT 'Simple:';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  n UInt32, x UInt32, y UInt32, z UInt32,
  projection p (
    SELECT count()
    GROUP BY x, z
  )
) ENGINE = MergeTree
order by tuple();

INSERT INTO tab
SELECT number, number % 3, number % 5, number % 7
FROM numbers_mt(30);

SELECT 'Projection cols used:';
SELECT DISTINCT x, z FROM tab;

EXPLAIN SELECT DISTINCT x, z FROM tab;

SELECT 'Some of projection cols used:';
SELECT DISTINCT z FROM tab;

EXPLAIN SELECT DISTINCT z FROM tab;

SELECT 'Not all cols in projection:';
SELECT DISTINCT x, y FROM tab;

EXPLAIN SELECT DISTINCT x, y FROM tab;

SELECT 'Expression in select:';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  n UInt32, x UInt32, y UInt32,
  projection p (
    SELECT count()
    GROUP BY x / 2, y % 10
  )
) ENGINE = MergeTree
order by tuple();

INSERT INTO tab
SELECT number, number % 3, number % 5
FROM numbers_mt(30);

SELECT 'Projection cols used:';
SELECT DISTINCT x / 2, y % 10 FROM tab;

EXPLAIN SELECT DISTINCT x / 2, y % 10 FROM tab;

SELECT 'Some of projection cols used:';
SELECT DISTINCT x / 2 FROM tab;

EXPLAIN SELECT DISTINCT x / 2 FROM tab;

SELECT 'Not all cols in projection:';
SELECT DISTINCT x / 2, y FROM tab;

EXPLAIN SELECT DISTINCT x / 2, y FROM tab;
