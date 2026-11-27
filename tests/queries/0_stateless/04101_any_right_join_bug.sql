-- Test: ANY RIGHT JOIN with multiple OR conditions (disjuncts) produces wrong results
-- when query_plan_join_swap_table=0 forces the RIGHT join path.

DROP TABLE IF EXISTS t1_04101;
DROP TABLE IF EXISTS t2_04101;

CREATE TABLE t1_04101 (a UInt32, b UInt32, v String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2_04101 (a UInt32, b UInt32, v String) ENGINE = MergeTree ORDER BY a;

-- L1 matches R1 via a=1 (disjunct 1), and L1 matches R2 via b=10 (disjunct 2)
INSERT INTO t1_04101 VALUES (1, 10, 'L1');
INSERT INTO t2_04101 VALUES (1, 99, 'R1'), (88, 10, 'R2');

-- With default table swap (rewritten to LEFT JOIN) - correct results
SELECT 'with_swap';
SELECT t1_04101.v, t2_04101.v
FROM t1_04101 ANY RIGHT JOIN t2_04101 ON t1_04101.a = t2_04101.a OR t1_04101.b = t2_04101.b
ORDER BY t2_04101.v;

-- Without table swap - RIGHT JOIN path exercised directly
SELECT 'without_swap';
SELECT t1_04101.v, t2_04101.v
FROM t1_04101 ANY RIGHT JOIN t2_04101 ON t1_04101.a = t2_04101.a OR t1_04101.b = t2_04101.b
ORDER BY t2_04101.v
SETTINGS query_plan_join_swap_table=0;

DROP TABLE IF EXISTS t1_04101;
DROP TABLE IF EXISTS t2_04101;
