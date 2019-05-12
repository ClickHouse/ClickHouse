SET enable_debug_queries = 1;

set allow_experimental_cross_to_join_conversion = 0;
select * from system.one cross join system.one;
set allow_experimental_cross_to_join_conversion = 1;
select * from system.one cross join system.one;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a Int8, b Nullable(Int8)) ENGINE = Memory;
CREATE TABLE t2 (a Int8, b Nullable(Int8)) ENGINE = Memory;

INSERT INTO t1 values (1,1), (2,2);
INSERT INTO t2 values (1,1), (1,2);
INSERT INTO t2 (a) values (2), (3);

SELECT 'cross';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1 cross join t2 where t1.a = t2.a;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 cross join t2 where t1.a = t2.a;
SELECT 'cross nullable';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1 cross join t2 where t1.b = t2.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 cross join t2 where t1.b = t2.b;
SELECT 'cross nullable vs not nullable';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1 cross join t2 where t1.a = t2.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 cross join t2 where t1.a = t2.b;
SELECT 'cross self';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1 x cross join t1 y where x.a = y.a and x.b = y.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 x cross join t1 y where x.a = y.a and x.b = y.b;
SELECT 'cross one table expr';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1 cross join t2 where t1.a = t1.b order by (t1.a, t2.a, t2.b);
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 cross join t2 where t1.a = t1.b order by (t1.a, t2.a, t2.b);
SELECT 'cross multiple ands';
SET allow_experimental_cross_to_join_conversion = 0;
--SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.a = t2.a and t1.b = t2.b and t1.a = t2.a;
SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b;
SELECT 'cross and inside and';
SET allow_experimental_cross_to_join_conversion = 0;
--SELECT * FROM t1 cross join t2 where t1.a = t2.a and (t1.a = t2.a and (t1.a = t2.a and t1.b = t2.b));
--SELECT * FROM t1 x cross join t2 y where t1.a = t2.a and (t1.b = t2.b and (x.a = y.a and x.b = y.b));
SELECT * FROM t1 cross join t2 where t1.a = t2.a and (t1.b = t2.b and 1);
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 cross join t2 where t1.a = t2.a and (t1.b = t2.b and 1);
SELECT 'cross split conjunction';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b and t1.a >= 1 and t2.b = 1;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b and t1.a >= 1 and t2.b = 1;

SET allow_experimental_cross_to_join_conversion = 1;

SELECT 'comma';
SELECT * FROM t1, t2 where t1.a = t2.a;
SELECT 'comma nullable';
SELECT * FROM t1, t2 where t1.b = t2.b;


SELECT 'cross';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a;
SELECT 'cross nullable';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1, t2 where t1.a = t2.a;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1, t2 where t1.a = t2.a;
SELECT 'cross nullable vs not nullable';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.b;
SELECT 'cross self';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1 x cross join t1 y where x.a = y.a and x.b = y.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1 x cross join t1 y where x.a = y.a and x.b = y.b;
SELECT 'cross one table expr';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t1.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t1.b;
SELECT 'cross multiple ands';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b;
SELECT 'cross and inside and';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a and (t1.a = t2.a and (t1.a = t2.a and t1.b = t2.b));
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a and (t1.a = t2.a and (t1.a = t2.a and t1.b = t2.b));

SELECT 'cross split conjunction';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b and t1.a >= 1 and t2.b > 0;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1 cross join t2 where t1.a = t2.a and t1.b = t2.b and t1.a >= 1 and t2.b > 0;

DROP TABLE t1;
DROP TABLE t2;
