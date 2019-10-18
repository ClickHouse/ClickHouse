SET enable_debug_queries = 1;
SET enable_optimize_predicate_expression = 0;

set allow_experimental_cross_to_join_conversion = 0;
select * from system.one l cross join system.one r;
set allow_experimental_cross_to_join_conversion = 1;
select * from system.one l cross join system.one r;

DROP TABLE IF EXISTS t1_00826;
DROP TABLE IF EXISTS t2_00826;

CREATE TABLE t1_00826 (a Int8, b Nullable(Int8)) ENGINE = Memory;
CREATE TABLE t2_00826 (a Int8, b Nullable(Int8)) ENGINE = Memory;

INSERT INTO t1_00826 values (1,1), (2,2);
INSERT INTO t2_00826 values (1,1), (1,2);
INSERT INTO t2_00826 (a) values (2), (3);

SELECT 'cross';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a;
SELECT 'cross nullable';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.b = t2_00826.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.b = t2_00826.b;
SELECT 'cross nullable vs not nullable';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.b;
SELECT 'cross self';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1_00826 x cross join t1_00826 y where x.a = y.a and x.b = y.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 x cross join t1_00826 y where x.a = y.a and x.b = y.b;
SELECT 'cross one table expr';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t1_00826.b order by (t1_00826.a, t2_00826.a, t2_00826.b);
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t1_00826.b order by (t1_00826.a, t2_00826.a, t2_00826.b);
SELECT 'cross multiple ands';
SET allow_experimental_cross_to_join_conversion = 0;
--SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and t1_00826.a = t2_00826.a;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b;
SELECT 'cross and inside and';
SET allow_experimental_cross_to_join_conversion = 0;
--SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b));
--SELECT * FROM t1_00826 x cross join t2_00826 y where t1_00826.a = t2_00826.a and (t1_00826.b = t2_00826.b and (x.a = y.a and x.b = y.b));
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and (t1_00826.b = t2_00826.b and 1);
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and (t1_00826.b = t2_00826.b and 1);
SELECT 'cross split conjunction';
SET allow_experimental_cross_to_join_conversion = 0;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and t1_00826.a >= 1 and t2_00826.b = 1;
SET allow_experimental_cross_to_join_conversion = 1;
SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and t1_00826.a >= 1 and t2_00826.b = 1;

SET allow_experimental_cross_to_join_conversion = 1;

SELECT 'comma';
SELECT * FROM t1_00826, t2_00826 where t1_00826.a = t2_00826.a;
SELECT 'comma nullable';
SELECT * FROM t1_00826, t2_00826 where t1_00826.b = t2_00826.b;


SELECT 'cross';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a;
SELECT 'cross nullable';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826, t2_00826 where t1_00826.a = t2_00826.a;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826, t2_00826 where t1_00826.a = t2_00826.a;
SELECT 'cross nullable vs not nullable';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.b;
SELECT 'cross self';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826 x cross join t1_00826 y where x.a = y.a and x.b = y.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826 x cross join t1_00826 y where x.a = y.a and x.b = y.b;
SELECT 'cross one table expr';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t1_00826.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t1_00826.b;
SELECT 'cross multiple ands';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b;
SELECT 'cross and inside and';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b));
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and (t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b));

SELECT 'cross split conjunction';
SET allow_experimental_cross_to_join_conversion = 0; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and t1_00826.a >= 1 and t2_00826.b > 0;
SET allow_experimental_cross_to_join_conversion = 1; ANALYZE SELECT * FROM t1_00826 cross join t2_00826 where t1_00826.a = t2_00826.a and t1_00826.b = t2_00826.b and t1_00826.a >= 1 and t2_00826.b > 0;

DROP TABLE t1_00826;
DROP TABLE t2_00826;
