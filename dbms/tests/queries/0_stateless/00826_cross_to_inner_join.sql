USE test;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a Int8, b Nullable(Int8)) ENGINE = Memory;
CREATE TABLE t2 (a Int8, b Nullable(Int8)) ENGINE = Memory;

INSERT INTO t1 values (1,1), (2,2);
INSERT INTO t2 values (1,1);
INSERT INTO t2 (a) values (2), (3);

SELECT 'cross';
SELECT * FROM t1 cross join t2 where t1.a = t2.a;
SELECT 'cross nullable';
SELECT * FROM t1 cross join t2 where t1.b = t2.b;
SELECT 'cross nullable vs not nullable';
SELECT * FROM t1 cross join t2 where t1.a = t2.b;

SET enable_debug_queries = 1;
AST SELECT * FROM t1 cross join t2 where t1.a = t2.a;
AST SELECT * FROM t1, t2 where t1.a = t2.a;

SET allow_experimental_cross_to_join_conversion = 1;

AST SELECT * FROM t1 cross join t2 where t1.a = t2.a;
AST SELECT * FROM t1, t2 where t1.a = t2.a;

SELECT 'cross';
SELECT * FROM t1 cross join t2 where t1.a = t2.a;
SELECT 'cross nullable';
SELECT * FROM t1 cross join t2 where t1.b = t2.b;
SELECT 'cross nullable vs not nullable';
SELECT * FROM t1 cross join t2 where t1.a = t2.b;

SELECT 'comma';
SELECT * FROM t1, t2 where t1.a = t2.a;
SELECT 'comma nullable';
SELECT * FROM t1, t2 where t1.b = t2.b;

DROP TABLE t1;
DROP TABLE t2;
