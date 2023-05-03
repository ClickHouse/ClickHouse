SET enable_optimize_predicate_expression = 0;
SET convert_query_to_cnf = 0;
SET cross_to_inner_join_rewrite = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (a UInt32, b Nullable(Int32)) ENGINE = Memory;
CREATE TABLE t2 (a UInt32, b Nullable(Int32)) ENGINE = Memory;
CREATE TABLE t3 (a UInt32, b Nullable(Int32)) ENGINE = Memory;
CREATE TABLE t4 (a UInt32, b Nullable(Int32)) ENGINE = Memory;

EXPLAIN SYNTAX SELECT t1.a FROM t1, t2;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2 WHERE t1.a = t2.a;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2 WHERE t1.b = t2.b;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3 WHERE t1.b = t2.b AND t1.b = t3.b;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3, t4 WHERE t1.b = t2.b AND t1.b = t3.b AND t1.b = t4.b;

EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3, t4 WHERE t2.a = t1.a AND t2.a = t3.a AND t2.a = t4.a;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3, t4 WHERE t3.a = t1.a AND t3.a = t2.a AND t3.a = t4.a;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3, t4 WHERE t4.a = t1.a AND t4.a = t2.a AND t4.a = t3.a;
EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a;

EXPLAIN SYNTAX SELECT t1.a FROM t1, t2, t3, t4;
EXPLAIN SYNTAX SELECT t1.a FROM t1 CROSS JOIN t2 CROSS JOIN t3 CROSS JOIN t4;

EXPLAIN SYNTAX SELECT t1.a FROM t1, t2 CROSS JOIN t3;
EXPLAIN SYNTAX SELECT t1.a FROM t1 JOIN t2 USING a CROSS JOIN t3;
EXPLAIN SYNTAX SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a CROSS JOIN t3;

INSERT INTO t1 values (1,1), (2,2), (3,3), (4,4);
INSERT INTO t2 values (1,1), (1, Null);
INSERT INTO t3 values (1,1), (1, Null);
INSERT INTO t4 values (1,1), (1, Null);

SELECT 'SELECT * FROM t1, t2';
SELECT * FROM t1, t2
ORDER BY t1.a, t2.b;
SELECT 'SELECT * FROM t1, t2 WHERE t1.a = t2.a';
SELECT * FROM t1, t2 WHERE t1.a = t2.a
ORDER BY t1.a, t2.b;
SELECT 'SELECT t1.a, t2.a FROM t1, t2 WHERE t1.b = t2.b';
SELECT t1.a, t2.b FROM t1, t2 WHERE t1.b = t2.b;
SELECT 'SELECT t1.a, t2.b, t3.b FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a';
SELECT t1.a, t2.b, t3.b FROM t1, t2, t3
WHERE t1.a = t2.a AND t1.a = t3.a
ORDER BY t2.b, t3.b;
SELECT 'SELECT t1.a, t2.b, t3.b FROM t1, t2, t3 WHERE t1.b = t2.b AND t1.b = t3.b';
SELECT t1.a, t2.b, t3.b FROM t1, t2, t3 WHERE t1.b = t2.b AND t1.b = t3.b;
SELECT 'SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a';
SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4
WHERE t1.a = t2.a AND t1.a = t3.a AND t1.a = t4.a
ORDER BY t2.b, t3.b, t4.b;
SELECT 'SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.b = t2.b AND t1.b = t3.b AND t1.b = t4.b';
SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4
WHERE t1.b = t2.b AND t1.b = t3.b AND t1.b = t4.b;
SELECT 'SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a';
SELECT t1.a, t2.b, t3.b, t4.b FROM t1, t2, t3, t4
WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a
ORDER BY t2.b, t3.b, t4.b;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
