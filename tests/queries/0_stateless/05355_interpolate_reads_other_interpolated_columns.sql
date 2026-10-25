-- An INTERPOLATE expression reads the previous interpolated value of another INTERPOLATE target it reads, unless several targets share that target's expression.

SELECT 'other target';
SELECT g, id, v FROM (SELECT toUInt16(number) AS g, toUInt32(number * 10) AS id, toUInt32(number) AS v FROM numbers(4))
ORDER BY g WITH FILL FROM 0 TO 7 INTERPOLATE (id AS id + v, v AS v * 2);

SELECT 'qualified';
SELECT g, id, v FROM (SELECT toUInt16(number) AS g, toUInt32(number * 10) AS id, toUInt32(number) AS v FROM numbers(4)) AS t
ORDER BY g WITH FILL FROM 0 TO 7 INTERPOLATE (id AS id + t.v, v AS t.v * 2);

SELECT 'alias of an expression';
SELECT g, id, v * 100 AS v FROM (SELECT toUInt16(number) AS g, toUInt32(number * 10) AS id, toUInt32(number) AS v FROM numbers(4))
ORDER BY g WITH FILL FROM 0 TO 7 INTERPOLATE (id AS id + v, v AS v + 1);

SELECT 'aggregates';
SELECT g, sum(x) AS s, count() AS c FROM (SELECT toUInt16(number * 4) AS g, toUInt64(number) AS x FROM numbers(3))
GROUP BY g ORDER BY g WITH FILL INTERPOLATE (s AS s + c, c AS c + 10);

SELECT 'swap';
SELECT g, a, b FROM (SELECT toUInt16(number) AS g, toUInt32(number) AS a, toUInt32(number + 100) AS b FROM numbers(4))
ORDER BY g WITH FILL FROM 0 TO 7 INTERPOLATE (a AS b, b AS a);

SELECT 'lambda';
SELECT g, id, v FROM (SELECT toUInt16(number) AS g, toUInt32(number * 10) AS id, toUInt32(number) AS v FROM numbers(4))
ORDER BY g WITH FILL FROM 0 TO 7 INTERPOLATE (id AS id + arraySum(arrayMap(x -> x + v, [toUInt32(0)])), v AS v * 2);

SELECT 'target dropped by the outer query';
SELECT g, id FROM
(
    SELECT g, id, v FROM (SELECT toUInt16(number) AS g, toUInt32(number * 10) AS id, toUInt32(number) AS v FROM numbers(4))
    ORDER BY g WITH FILL FROM 0 TO 7 INTERPOLATE (id AS id + v, v AS v * 2)
);

SELECT 'remote';
DROP TABLE IF EXISTS t_interpolate;
CREATE TABLE t_interpolate (g UInt16, id UInt32, v UInt32) ENGINE = Memory;
INSERT INTO t_interpolate VALUES (0, 0, 0), (1, 10, 1), (2, 20, 2), (3, 30, 3);
SELECT g, id, v FROM remote('127.0.0.2', currentDatabase(), t_interpolate)
ORDER BY g WITH FILL FROM 0 TO 7 INTERPOLATE (id AS id + v, v AS v * 2);
DROP TABLE t_interpolate;

SELECT 'constant target';
SELECT toUInt16(number * 3) AS g, 5 AS d, toUInt32(number) AS x FROM numbers(2)
ORDER BY g WITH FILL INTERPOLATE (d AS d + 1, x AS x + d);

SELECT 'constant target dropped by the outer query';
SELECT g, x FROM
(
    SELECT toUInt16(number * 3) AS g, 5 AS d, toUInt32(number) AS x FROM numbers(2)
    ORDER BY g WITH FILL INTERPOLATE (d AS d + 1, x AS x + d)
);

SELECT 'constant target declared after its reader';
SELECT toUInt16(number * 3) AS g, 5 AS d, toUInt32(number) AS x FROM numbers(2)
ORDER BY g WITH FILL INTERPOLATE (x AS x + d, d AS d + 1);

SELECT 'the fill column is not a target';
SELECT g, g AS h, x FROM (SELECT toUInt16(number * 3) AS g, toUInt32(number) AS x FROM numbers(2))
ORDER BY g WITH FILL INTERPOLATE (h AS h + 10, x AS x + g);

SELECT 'a literal is not a target';
SELECT toUInt16(number * 3) AS g, 5 AS d, toUInt32(number) AS x FROM numbers(2)
ORDER BY g WITH FILL INTERPOLATE (d AS d + 1, x AS x + 5);

SELECT 'a shared expression is not rebound';
SELECT g, x AS a, x AS b FROM (SELECT toUInt16(number * 4) AS g, toUInt32(number + 1) AS x FROM numbers(2))
ORDER BY g WITH FILL INTERPOLATE (a AS 0, b AS b + a);

SELECT 'a shared expression is not rebound, reversed';
SELECT g, x AS a, x AS b FROM (SELECT toUInt16(number * 4) AS g, toUInt32(number + 1) AS x FROM numbers(2))
ORDER BY g WITH FILL INTERPOLATE (b AS b + a, a AS 0);

SELECT 'a fill column read keeps no target';
SELECT g, x FROM
(
    SELECT g, g AS h, toUInt32(number) AS x FROM (SELECT toUInt16(number * 3) AS g, number FROM numbers(2))
    ORDER BY g WITH FILL INTERPOLATE (h AS intDiv(h, h - h), x AS x + g)
);

SELECT 'a shared expression read keeps no target';
SELECT g, c FROM
(
    SELECT g, x AS a, x AS b, toUInt32(number) AS c FROM (SELECT toUInt16(number * 3) AS g, toUInt32(number + 1) AS x, number FROM numbers(2))
    ORDER BY g WITH FILL INTERPOLATE (a AS intDiv(a, a - a), b AS b, c AS c + a)
);

SELECT 'equal columns of different tables are different expressions';
SELECT g, c FROM
(
    SELECT toUInt16(l.number * 3) AS g, l.x AS a, r.x AS b, toUInt32(l.number) AS c
    FROM (SELECT number, toUInt32(number + 1) AS x FROM numbers(2)) AS l
    INNER JOIN (SELECT number, toUInt32(number + 1) AS x FROM numbers(2)) AS r ON l.number = r.number
    ORDER BY g WITH FILL INTERPOLATE (a AS a * 2, b AS intDiv(b, b - b), c AS c + l.x)
);

SELECT 'a target named like another column';
SELECT n, count() AS c, toUInt64(n * 2) AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3))
GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 1, c AS c + `count()`);

SELECT 'a constant target named like another column';
SELECT n, count() AS c, 5 AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3))
GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 1, c AS c + `count()`);

SELECT 'a chain through a constant target dropped by the outer query';
SELECT g, x FROM
(
    SELECT g, v, 5 AS d, x FROM (SELECT toUInt16(number * 4) AS g, toUInt32(number + 1) AS v, toUInt32(number) AS x FROM numbers(2))
    ORDER BY g WITH FILL INTERPOLATE (v AS v * 2, d AS d + v, x AS x + d)
);

SELECT 'an alias column read keeps no target';
DROP TABLE IF EXISTS t_interpolate_alias;
CREATE TABLE t_interpolate_alias (g UInt16, x UInt32, c UInt32, z UInt32 ALIAS x + 1) ENGINE = Memory;
INSERT INTO t_interpolate_alias VALUES (0, 0, 0), (3, 1, 1);
SELECT g, c FROM
(
    SELECT g, x + 1 AS a, c FROM t_interpolate_alias
    ORDER BY g WITH FILL INTERPOLATE (a AS intDiv(a, a - a), c AS c + z)
);
DROP TABLE t_interpolate_alias;

SELECT 'a UNION ALL branch keeps no target for another branch';
SELECT g, c FROM
(
    SELECT g, a, c FROM (SELECT toUInt16(number * 3) AS g, toInt32(number + 1) AS a, toInt32(number) AS c FROM numbers(2))
    ORDER BY g WITH FILL INTERPOLATE (a AS a * 2, c AS c + a)
    UNION ALL
    SELECT g, a, c FROM (SELECT toUInt16(number * 3 + 10) AS g, toInt32(number + 1) AS a, toInt32(number) AS c FROM numbers(2))
    ORDER BY g WITH FILL INTERPOLATE (a AS intDiv(a, a - a), c AS c + 1)
)
ORDER BY g;
