

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 ( x Int ) Engine = Memory;
INSERT INTO t1 VALUES ( 1 ), ( 2 ), ( 3 );

CREATE TABLE t2 ( x Int ) Engine = Memory;
INSERT INTO t2 VALUES ( 2 ), ( 3 ), ( 4 );

SET cross_to_inner_join_rewrite = 1;
SELECT count() = 1 FROM t1, t2 WHERE t1.x > t2.x;
SELECT count() = 2 FROM t1, t2 WHERE t1.x = t2.x;
SELECT count() = 2 FROM t1 CROSS JOIN t2 WHERE t1.x = t2.x;
SELECT count() = 1 FROM t1 CROSS JOIN t2 WHERE t1.x > t2.x;

SET cross_to_inner_join_rewrite = 2;
SELECT count() = 1 FROM t1, t2 WHERE t1.x > t2.x; -- { serverError INCORRECT_QUERY }
SELECT count() = 2 FROM t1, t2 WHERE t1.x = t2.x;
SELECT count() = 2 FROM t1 CROSS JOIN t2 WHERE t1.x = t2.x;
SELECT count() = 1 FROM t1 CROSS JOIN t2 WHERE t1.x > t2.x; -- do not force rewrite explicit CROSS
