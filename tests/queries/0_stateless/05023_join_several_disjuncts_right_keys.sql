-- A join with several disjuncts adds to the result the right key columns the query asks for, and only
-- those. A key value has to come from the matched right row: with `a = a OR b = b` a row matched through
-- the second clause has a right `a` of its own, unrelated to the left one.

CREATE TABLE l (a UInt64, b UInt64, lp String) ENGINE = Memory;
CREATE TABLE r (a UInt64, b UInt64, rp String) ENGINE = Memory;

INSERT INTO l VALUES (1, 10, 'l1'), (2, 20, 'l2'), (3, 30, 'l3');
-- (7, 20) matches l(2, 20) through `b` alone, so its right `a` is 7, not 2. (9, 90) matches nothing,
-- so a RIGHT or FULL join has a non-joined right row to emit.
INSERT INTO r VALUES (1, 99, 'r1'), (7, 20, 'r2'), (9, 90, 'r3');

SELECT '-- both keys selected: the right value comes from the matched row';
SELECT l.a, l.b, r.a, r.b FROM l ALL LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;

SELECT '-- one key selected';
SELECT l.a, r.b FROM l ALL LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;

SELECT '-- no right column selected';
SELECT l.a FROM l ALL LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;
SELECT count() FROM l ALL LEFT JOIN r ON l.a = r.a OR l.b = r.b;

SELECT '-- a right payload column, keys not selected';
SELECT l.a, r.rp FROM l ALL LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;

SELECT '-- INNER';
SELECT l.a, r.a, r.rp FROM l ALL INNER JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;
SELECT count() FROM l ALL INNER JOIN r ON l.a = r.a OR l.b = r.b;

SELECT '-- RIGHT, which emits the non-joined right rows too';
SELECT l.a, r.a, r.rp FROM l ALL RIGHT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY r.a;
SELECT count() FROM l ALL RIGHT JOIN r ON l.a = r.a OR l.b = r.b;
-- no right key selected, so none is added to the result, and the non-joined right row still comes out
SELECT l.a, r.rp FROM l ALL RIGHT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY r.rp;

SELECT '-- FULL';
SELECT l.a, r.a FROM l ALL FULL JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a, r.a;
SELECT count() FROM l ALL FULL JOIN r ON l.a = r.a OR l.b = r.b;

SELECT '-- ANY';
SELECT l.a, r.a FROM l ANY LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;

SELECT '-- SEMI and ANTI, which emit the left row alone';
SELECT l.a FROM l SEMI LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;
SELECT count() FROM l SEMI LEFT JOIN r ON l.a = r.a OR l.b = r.b;
SELECT l.a FROM l ANTI LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a;
SELECT count() FROM l ANTI LEFT JOIN r ON l.a = r.a OR l.b = r.b;

SELECT '-- a residual condition reads a right column that nothing else selects';
-- the old analyzer does not support a residual condition next to the disjuncts
SELECT l.a, l.lp FROM l ALL LEFT JOIN r ON (l.a = r.a OR l.b = r.b) AND l.lp < r.rp ORDER BY l.a SETTINGS enable_analyzer = 1;

SELECT '-- and with join_use_nulls';
SELECT l.a, r.a FROM l ALL LEFT JOIN r ON l.a = r.a OR l.b = r.b ORDER BY l.a SETTINGS join_use_nulls = 1;

DROP TABLE l;
DROP TABLE r;
