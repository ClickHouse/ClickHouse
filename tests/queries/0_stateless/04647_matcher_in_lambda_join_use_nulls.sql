DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS lc2;
DROP TABLE IF EXISTS ar2;
DROP TABLE IF EXISTS u1;
DROP TABLE IF EXISTS u2;
DROP TABLE IF EXISTS s1;
DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;
DROP TABLE IF EXISTS p1;
DROP TABLE IF EXISTS p2;

CREATE TABLE t1 (a UInt64) ENGINE = Memory;
CREATE TABLE t2 (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t1 VALUES (1), (9);
INSERT INTO t2 VALUES (1, 2);

SELECT '-- 1. LEFT JOIN, matcher in lambda, must match the explicit-column oracle';
SELECT t1.a, arrayMap(z -> sipHash64(b.x, b.y), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 2. FULL JOIN, plain table and derived table';
SELECT t1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM t1 FULL JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM t1 FULL JOIN (SELECT x, y FROM t2) AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 3. other capture-producing higher-order functions';
SELECT t1.a, arrayFilter(z -> sipHash64(b.*) > 0, [1]) AS r FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> tuple(b.*), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 4. matcher flavours: unqualified *, COLUMNS, EXCEPT, unaliased table name';
SELECT t1.a, arrayMap(z -> tuple(*), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(b.COLUMNS('x')), [1]) AS r FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(b.* EXCEPT y), [1]) AS r FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(t2.*), [1]) AS r FROM t1 LEFT JOIN t2 ON t1.a = t2.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 5. nested lambdas';
SELECT t1.a, arrayMap(p -> arrayMap(z -> sipHash64(b.*), [1]), [1]) AS r FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 6. matcher in lambda inside a subquery that owns the join';
SELECT * FROM (SELECT t1.a AS k, arrayMap(z -> sipHash64(b.*), [1]) AS r FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x) ORDER BY k SETTINGS join_use_nulls = 1;
SELECT count() FROM t1 WHERE t1.a IN (SELECT arrayMap(z -> sipHash64(bb.x, bb.y), [1])[1] % 2 FROM t1 AS aa LEFT JOIN t2 AS bb ON aa.a = bb.x) SETTINGS join_use_nulls = 1;
SELECT count() FROM t1 WHERE t1.a IN (SELECT arrayMap(z -> sipHash64(bb.*), [1])[1] % 2 FROM t1 AS aa LEFT JOIN t2 AS bb ON aa.a = bb.x) SETTINGS join_use_nulls = 1;

SELECT '-- 6b. an inner subquery must NOT adopt the outer query join promotion';
CREATE TABLE s1 (p UInt64) ENGINE = Memory;
INSERT INTO s1 VALUES (1), (5);
SELECT t1.a, (SELECT arrayMap(z -> tuple(s1.p), [1]) FROM s1 LIMIT 1) AS r, toTypeName(r) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, (SELECT arrayMap(z -> tuple(s1.*), [1]) FROM s1 LIMIT 1) AS r, toTypeName(r) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 7. type wrappers: LowCardinality promoted, Array left alone';
CREATE TABLE lc2 (x UInt64, s LowCardinality(String)) ENGINE = Memory;
INSERT INTO lc2 VALUES (1, 'p');
SELECT t1.a, arrayMap(z -> tuple(b.s), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN lc2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> tuple(b.* EXCEPT x), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN lc2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
CREATE TABLE ar2 (x UInt64, arr Array(UInt64)) ENGINE = Memory;
INSERT INTO ar2 VALUES (1, [7]);
SELECT t1.a, arrayMap(z -> tuple(b.arr), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN ar2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> tuple(b.* EXCEPT x), [1]) AS r, toTypeName(r) FROM t1 LEFT JOIN ar2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

-- 8. intentionally absent: the explicit-column equivalence oracle is paired with every matcher
-- case above rather than being asserted as a section of its own.

SELECT '-- 9. controls: unpromoted sides and settings must be unchanged';
SELECT t1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM t1 RIGHT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM t1 INNER JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM t1 CROSS JOIN t2 AS b ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM t1 FULL JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 0;
SELECT t1.a, sipHash64(b.*) AS r, toTypeName(r) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 9b. USING join is reconciled by its own mechanism, must not regress';
CREATE TABLE u1 (a UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE u2 (a UInt64, w UInt64) ENGINE = Memory;
INSERT INTO u1 VALUES (1, 10), (9, 90);
INSERT INTO u2 VALUES (1, 11);
SELECT u1.a, arrayMap(z -> tuple(u2.a, u2.w), [1]) AS r, toTypeName(r) FROM u1 LEFT JOIN u2 USING (a) ORDER BY u1.a SETTINGS join_use_nulls = 1;
SELECT u1.a, arrayMap(z -> tuple(u2.*), [1]) AS r, toTypeName(r) FROM u1 LEFT JOIN u2 USING (a) ORDER BY u1.a SETTINGS join_use_nulls = 1;

SELECT '-- 10. group_by_use_nulls with ROLLUP / CUBE / GROUPING SETS';
CREATE TABLE t3 (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t3 VALUES (1, 2), (3, 4);
SET group_by_use_nulls = 1;
SELECT x, y, arrayMap(z -> sipHash64(t3.x, t3.y), [1]) AS r FROM t3 GROUP BY x, y WITH ROLLUP ORDER BY x, y;
SELECT x, y, arrayMap(z -> sipHash64(t3.*), [1]) AS r FROM t3 GROUP BY x, y WITH ROLLUP ORDER BY x, y;
SELECT x, y, arrayMap(z -> sipHash64(t3.*), [1]) AS r FROM t3 GROUP BY x, y WITH CUBE ORDER BY x, y;
SELECT x, y, arrayMap(z -> sipHash64(t3.*), [1]) AS r FROM t3 GROUP BY GROUPING SETS ((x, y), (x)) ORDER BY x, y;

SELECT '-- 10b. APPLY carve-out: aggregate created by APPLY must not be promoted';
SELECT x, * APPLY q -> argMax(q, y) FROM t3 GROUP BY x WITH ROLLUP ORDER BY x;

SELECT '-- 11. aggregate context: group_by_use_nulls keys must NOT reach into an aggregate argument';
SELECT x, y, sum(arrayMap(z -> sipHash64(t3.x, t3.y), [1])[1]) AS s FROM t3 GROUP BY x, y WITH ROLLUP ORDER BY x, y;
SELECT x, y, sum(arrayMap(z -> sipHash64(t3.*), [1])[1]) AS s FROM t3 GROUP BY x, y WITH ROLLUP ORDER BY x, y;
SELECT x, y, sum(sipHash64(t3.*)) AS s FROM t3 GROUP BY x, y WITH ROLLUP ORDER BY x, y;
SELECT x, y, groupArray(arrayMap(z -> sipHash64(t3.*), [1])[1]) AS g FROM t3 GROUP BY x, y WITH ROLLUP ORDER BY x, y;
SET group_by_use_nulls = 0;

SELECT '-- 11b. join_use_nulls IS pre-aggregation, so it MUST reach into an aggregate argument';
SELECT sum(arrayMap(z -> sipHash64(b.x, b.y), [1])[1]) AS s FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x SETTINGS join_use_nulls = 1;
SELECT sum(arrayMap(z -> sipHash64(b.*), [1])[1]) AS s FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x SETTINGS join_use_nulls = 1;
SELECT sum(sipHash64(b.*)) AS s, toTypeName(s) FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x SETTINGS join_use_nulls = 1;
SELECT t1.a, sum(arrayMap(z -> sipHash64(b.x, b.y), [1])[1]) OVER () AS w FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;
SELECT t1.a, sum(arrayMap(z -> sipHash64(b.*), [1])[1]) OVER () AS w FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x ORDER BY t1.a SETTINGS join_use_nulls = 1;

SELECT '-- 12. PREWHERE expressions must keep pre-join types';
CREATE TABLE m1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE m2 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO m1 VALUES (1), (9);
INSERT INTO m2 VALUES (1, 2);
SELECT count() FROM m1 RIGHT JOIN m2 AS b ON m1.a = b.x PREWHERE arrayMap(z -> sipHash64(m1.*), [1])[1] > 0 SETTINGS join_use_nulls = 1;
SELECT count() FROM m1 LEFT JOIN m2 AS b ON m1.a = b.x PREWHERE arrayMap(z -> sipHash64(m1.*), [1])[1] > 0 SETTINGS join_use_nulls = 1;
SELECT count() FROM m1 RIGHT JOIN m2 AS b ON m1.a = b.x PREWHERE arrayMap(z -> sipHash64(m1.*), [1])[1] > 0 SETTINGS join_use_nulls = 0;
SELECT count() FROM m1 RIGHT JOIN m2 AS b ON m1.a = b.x PREWHERE arrayMap(z -> sipHash64(m1.a), [1])[1] > 0 SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> sipHash64(m1.a), [1])[1] AS c, toTypeName(c) FROM m1 RIGHT JOIN m2 AS b ON m1.a = b.x PREWHERE c > 0 SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> sipHash64(m1.*), [1])[1] AS c, toTypeName(c) FROM m1 RIGHT JOIN m2 AS b ON m1.a = b.x PREWHERE c > 0 SETTINGS join_use_nulls = 1;

SELECT '-- 12b. WHERE runs after the join, so it MUST still be promoted';
SELECT m1.a, arrayMap(z -> sipHash64(b.x, b.y), [1]) AS r, toTypeName(r) FROM m1 LEFT JOIN m2 AS b ON m1.a = b.x WHERE m1.a > 0 ORDER BY m1.a SETTINGS join_use_nulls = 1;
SELECT m1.a, arrayMap(z -> sipHash64(b.*), [1]) AS r, toTypeName(r) FROM m1 LEFT JOIN m2 AS b ON m1.a = b.x WHERE m1.a > 0 ORDER BY m1.a SETTINGS join_use_nulls = 1;

SELECT '-- 12c. a subquery nested in PREWHERE keeps its own join_use_nulls handling';
SELECT count() FROM m1 PREWHERE m1.a IN (SELECT arrayMap(z -> sipHash64(b.x, b.y), [1])[1] % 2 FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x) SETTINGS join_use_nulls = 1;
SELECT count() FROM m1 PREWHERE m1.a IN (SELECT arrayMap(z -> sipHash64(b.*), [1])[1] % 2 FROM t1 LEFT JOIN t2 AS b ON t1.a = b.x) SETTINGS join_use_nulls = 1;

SELECT '-- 12d. USING join, unqualified matcher in a lambda, RIGHT/FULL: with and without PREWHERE';
CREATE TABLE p1 (a UInt8, v UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE p2 (a UInt8, w UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO p1 VALUES (1, 10), (9, 90);
INSERT INTO p2 VALUES (1, 11);
SELECT arrayMap(z -> tuple(a, v), [1]) AS r, toTypeName(r) FROM p1 RIGHT JOIN p2 USING (a) PREWHERE p1.v > 0 ORDER BY r SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> tuple(* EXCEPT w), [1]) AS r, toTypeName(r) FROM p1 RIGHT JOIN p2 USING (a) PREWHERE p1.v > 0 ORDER BY r SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> tuple(a, v), [1]) AS r, toTypeName(r) FROM p1 FULL JOIN p2 USING (a) PREWHERE p1.v > 0 ORDER BY r SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> tuple(* EXCEPT w), [1]) AS r, toTypeName(r) FROM p1 FULL JOIN p2 USING (a) PREWHERE p1.v > 0 ORDER BY r SETTINGS join_use_nulls = 1;
-- The PREWHERE above is not what makes these abort on the unfixed analyzer: the same queries
-- abort without it, so the pre-join-type rollback of 12. is not the mechanism under test here.
SELECT arrayMap(z -> tuple(a, v), [1]) AS r, toTypeName(r) FROM p1 RIGHT JOIN p2 USING (a) ORDER BY r SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> tuple(* EXCEPT w), [1]) AS r, toTypeName(r) FROM p1 RIGHT JOIN p2 USING (a) ORDER BY r SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> tuple(a, v), [1]) AS r, toTypeName(r) FROM p1 FULL JOIN p2 USING (a) ORDER BY r SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> tuple(* EXCEPT w), [1]) AS r, toTypeName(r) FROM p1 FULL JOIN p2 USING (a) ORDER BY r SETTINGS join_use_nulls = 1;
SELECT arrayMap(z -> tuple(* EXCEPT w), [1]) AS r, toTypeName(r) FROM p1 RIGHT JOIN p2 USING (a) ORDER BY r SETTINGS join_use_nulls = 0;
SELECT arrayMap(z -> tuple(* EXCEPT w), [1]) AS r, toTypeName(r) FROM p1 LEFT JOIN p2 USING (a) ORDER BY r SETTINGS join_use_nulls = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE lc2;
DROP TABLE ar2;
DROP TABLE u1;
DROP TABLE u2;
DROP TABLE s1;
DROP TABLE m1;
DROP TABLE m2;
DROP TABLE p1;
DROP TABLE p2;
