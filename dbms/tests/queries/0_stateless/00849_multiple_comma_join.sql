SET enable_debug_queries = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (a UInt32, b Nullable(Int32)) ENGINE = Memory;
CREATE TABLE t2 (a UInt32, b Nullable(Int32)) ENGINE = Memory;
CREATE TABLE t3 (a UInt32, b Nullable(Int32)) ENGINE = Memory;
CREATE TABLE t4 (a UInt32, b Nullable(Int32)) ENGINE = Memory;

ANALYZE SELECT t1.a FROM t1, t2;
ANALYZE SELECT t1.a FROM t1, t2 WHERE t1.a = t2.a;
ANALYZE SELECT t1.a FROM t1, t2 WHERE t1.b = t2.b;
ANALYZE SELECT t1.a FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a;
ANALYZE SELECT t1.a FROM t1, t2, t3 WHERE t1.b = t2.b AND t1.b = t3.b;
ANALYZE SELECT t1.a FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a;
ANALYZE SELECT t1.a FROM t1, t2, t3, t4 WHERE t1.b = t2.b AND t1.b = t3.b AND t1.b = t4.b;

ANALYZE SELECT t1.a FROM t1, t2, t3, t4 WHERE t2.a = t1.a AND t2.a = t3.a AND t2.a = t4.a;
ANALYZE SELECT t1.a FROM t1, t2, t3, t4 WHERE t3.a = t1.a AND t3.a = t2.a AND t3.a = t4.a;
ANALYZE SELECT t1.a FROM t1, t2, t3, t4 WHERE t4.a = t1.a AND t4.a = t2.a AND t4.a = t3.a;
ANALYZE SELECT t1.a FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a;

ANALYZE SELECT t1.a FROM t1, t2, t3, t4;
ANALYZE SELECT t1.a FROM t1 CROSS JOIN t2 CROSS JOIN t3 CROSS JOIN t4;

ANALYZE SELECT t1.a FROM t1, t2 CROSS JOIN t3; -- { serverError 48 }
ANALYZE SELECT t1.a FROM t1 JOIN t2 USING a CROSS JOIN t3; -- { serverError 48 }
ANALYZE SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a CROSS JOIN t3;

INSERT INTO t1 values (1,1), (2,2), (3,3), (4,4);
INSERT INTO t2 values (1,1), (1, Null);
INSERT INTO t3 values (1,1), (1, Null);
INSERT INTO t4 values (1,1), (1, Null);

SELECT 'SELECT * FROM t1, t2';
SELECT * FROM t1, t2;
SELECT 'SELECT * FROM t1, t2 WHERE t1.a = t2.a';
SELECT * FROM t1, t2 WHERE t1.a = t2.a;
SELECT 'SELECT t1.a, t2.a FROM t1, t2 WHERE t1.b = t2.b';
SELECT t1.a, t2.b FROM t1, t2 WHERE t1.b = t2.b;
SELECT 'SELECT t1.a, t2.b, t3.b FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a';
SELECT t1.a, t2.b, t3.b FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a;
SELECT 'SELECT t1.a, t2.b, t3.b FROM t1, t2, t3 WHERE t1.b = t2.b AND t1.b = t3.b';
SELECT t1.a, t2.b, t3.b FROM t1, t2, t3 WHERE t1.b = t2.b AND t1.b = t3.b;
SELECT 'SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a';
SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a;
SELECT 'SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.b = t2.b AND t1.b = t3.b AND t1.b = t4.b';
SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.b = t2.b AND t1.b = t3.b AND t1.b = t4.b;
SELECT 'SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a';
SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
