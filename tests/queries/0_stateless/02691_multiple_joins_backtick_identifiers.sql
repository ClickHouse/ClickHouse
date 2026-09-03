DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (`1a` Nullable(Int64), `2b` Nullable(String)) engine = Memory;
CREATE TABLE t2 (`3c` Nullable(Int64), `4d` Nullable(String)) engine = Memory;
CREATE TABLE t3 (`5e` Nullable(Int64), `6f` Nullable(String)) engine = Memory;

SELECT
    `1a`,
    `2b`
FROM t1 AS tt1
INNER JOIN
(
    SELECT `3c`
    FROM t2
) AS tt2 ON tt1.`1a` = tt2.`3c`
INNER JOIN
(
    SELECT `6f`
    FROM t3
) AS tt3 ON tt1.`2b` = tt3.`6f`;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (`a` Nullable(Int64), `b` Nullable(String)) engine = Memory;
CREATE TABLE t2 (`c` Nullable(Int64), `d` Nullable(String)) engine = Memory;
CREATE TABLE t3 (`e` Nullable(Int64), `f` Nullable(String)) engine = Memory;

SELECT
    a,
    b
FROM t1 AS tt1
INNER JOIN
(
    SELECT c
    FROM t2
) AS tt2 ON tt1.a = tt2.c
INNER JOIN
(
    SELECT f
    FROM t3
) AS tt3 ON tt1.b = tt3.f;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
