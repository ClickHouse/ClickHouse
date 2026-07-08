-- Ported from DuckDB test/sql/join/iejoin/test_iejoin_null_keys.test (their issue #10122):
-- rows with NULLs in the keys must not produce matches.
-- The original uses LEFT joins; a LEFT join with an inequality-only ON section is not
-- supported in ClickHouse yet, so the matching part is checked with INNER joins and the
-- LEFT join is locked to the current error.

SET allow_experimental_ie_join = 1;

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

SELECT * FROM tt t1 LEFT JOIN tt t2 ON t1.x < t2.x AND t1.y < t2.y ORDER BY ALL; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE tt;
DROP TABLE tt2;
