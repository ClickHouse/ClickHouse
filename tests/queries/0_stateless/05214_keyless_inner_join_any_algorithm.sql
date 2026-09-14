-- An `INNER` join whose condition yields no key is a cross product with a filter. `ConstantJoin`
-- executes the cross product whichever `join_algorithm` is set, so no algorithm is required for it.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t3 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1, 2), (3, 4), (5, 6);
INSERT INTO t2 VALUES (3, 4), (5, 6), (7, 8);
INSERT INTO t3 VALUES (5, 6), (7, 8), (9, 10);

SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';

SELECT '-- keyless ON condition';
SELECT * FROM t1 JOIN t2 ON t1.a + t2.b = 7 ORDER BY ALL;

SELECT '-- hyperedge of a comma join, the reordering makes it a key';
SELECT * FROM t1, t2, t3 WHERE t1.a + t3.a = t2.a ORDER BY ALL;
SELECT countIf(explain ILIKE '%Type: INNER%'), countIf(explain ILIKE '%Type: CROSS%')
FROM (EXPLAIN actions = 1 SELECT * FROM t1, t2, t3 WHERE t1.a + t3.a = t2.a);

SELECT '-- the same with the hash join';
SELECT * FROM t1 JOIN t2 ON t1.a + t2.b = 7 ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT * FROM t1, t2, t3 WHERE t1.a + t3.a = t2.a ORDER BY ALL SETTINGS join_algorithm = 'hash';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
