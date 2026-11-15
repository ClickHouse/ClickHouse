DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 ( `a1` Int64, `1a1` Int64 ) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 11);

CREATE TABLE t2 ( `b1` Int64, `1b1` Int64 ) ENGINE = Memory;
INSERT INTO t2 VALUES (1, 12);

CREATE TABLE t3 ( `c1` Int64, `1c1` Int64 ) ENGINE = Memory;
INSERT INTO t3 VALUES (1, 13);

SELECT
    *
FROM t1 AS t1
INNER JOIN t2 AS t2 ON t1.a1 = t2.b1
INNER JOIN t3 AS t3 ON t1.a1 = t3.c1;

SELECT t2.`1b1` FROM t1 JOIN t2 ON a1 = b1;
SELECT `1b1` FROM t1 JOIN t2 ON a1 = b1;

SELECT
    `字段a1`, `$1a1`, `字段b1`, `@res`.`1b1`
FROM (
        SELECT
            t1.`a1` AS `字段a1`, `1a1` AS `$1a1`, t2.`b1` + 1 AS `字段b1`, `表2`.`1b1`
        FROM t1
        INNER JOIN t2 AS `表2` on t1.a1 = `表2`.b1
    ) AS `@res`;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
