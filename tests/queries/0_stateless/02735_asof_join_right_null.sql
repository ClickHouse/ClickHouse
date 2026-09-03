
CREATE TABLE t1 (a Int, b Int) ENGINE = Memory;
INSERT INTO t1 VALUES (1, -1), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4);

CREATE TABLE t2 (a Int, b Nullable(Int)) ENGINE = Memory;
INSERT INTO t2 VALUES (1, 1), (1, NULL), (1, 2);

-- { echoOn }
SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b < t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b <= t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b > t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b >= t2.b ORDER BY t1.b;

SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b < t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b <= t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b > t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b >= t2.b ORDER BY t1.b;

SET join_use_nulls = 1;

SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b < t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b <= t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b > t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.b >= t2.b ORDER BY t1.b;

SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b < t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b <= t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b > t2.b ORDER BY t1.b;
SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.a = t2.a AND t1.b >= t2.b ORDER BY t1.b;

DROP TABLE t1;

