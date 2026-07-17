-- Tags: no-old-analyzer

-- Rows with NULLs in the keys must not produce matches.
-- The matching part is checked with INNER joins; LEFT joins are checked with
-- `join_use_nulls = 1` (rows with NULL keys are unmatched, not dropped).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS tt;
DROP TABLE IF EXISTS tt2;

CREATE TABLE tt (x Nullable(Int32), y Nullable(Int32), z Int32) ENGINE = MergeTree ORDER BY z;
INSERT INTO tt SELECT nullIf(number % 3, 0), nullIf(number % 5, 0), number FROM numbers(10);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT * FROM tt t1 JOIN tt t2 ON t1.x < t2.x AND t1.y < t2.y) WHERE explain LIKE '%IEJoin%';
SELECT * FROM tt t1 JOIN tt t2 ON t1.x < t2.x AND t1.y < t2.y ORDER BY t1.z, t2.z;

-- All-NULL first key and a constant second key coming from a subquery
CREATE TABLE tt2 (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO tt2 SELECT number FROM numbers(10);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT if(x < 100, NULL, 99) AS x, if(x < 100, 99, 99) AS y FROM tt2) t1 JOIN tt2 t2 ON t1.x < t2.x AND t1.y < t2.x) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM (SELECT if(x < 100, NULL, 99) AS x, if(x < 100, 99, 99) AS y FROM tt2) t1 JOIN tt2 t2 ON t1.x < t2.x AND t1.y < t2.x;

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT * FROM tt t1 LEFT JOIN tt t2 ON t1.x < t2.x AND t1.y < t2.y SETTINGS join_use_nulls = 1) WHERE explain LIKE '%IEJoin%';
SELECT * FROM tt t1 LEFT JOIN tt t2 ON t1.x < t2.x AND t1.y < t2.y ORDER BY t1.x NULLS FIRST, t1.y NULLS FIRST, t1.z, t2.x, t2.y, t2.z SETTINGS join_use_nulls = 1;

-- The all-NULL first key with a LEFT join: every left row is unmatched and padded
SELECT t1.x, t1.y FROM (SELECT if(x < 100, NULL, 99) AS x, if(x < 100, 99, 99) AS y FROM tt2) t1 LEFT JOIN tt2 t2 ON t1.x < t2.x AND t1.y < t2.x ORDER BY t1.x NULLS FIRST, t1.y NULLS FIRST SETTINGS join_use_nulls = 1;

DROP TABLE tt;
DROP TABLE tt2;
