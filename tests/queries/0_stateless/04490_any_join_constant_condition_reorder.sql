-- Test for a regression where join order optimization converted `ANY INNER JOIN ... ON 1`
-- (constant-true condition, empty join expression, hence an unconnected pair in the join graph)
-- to a CROSS join, silently dropping ANY strictness and returning a full cartesian product.
-- The correct plan is an inner ANY join on the injected constant key (`1 = 1`).

SET enable_analyzer = 1;
SET join_algorithm = 'hash';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;

SELECT '-- ANY INNER JOIN ON constant true: one row per (constant) key';
SELECT * FROM values('l UInt8', 1, 2) AS l_tbl ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1 ORDER BY ALL;

SELECT '-- plan keeps inner/any with constant key, not cross';
SELECT extract(explain, 'Type: \\w+ \\| Strictness: \\w+') FROM (
    EXPLAIN SELECT * FROM values('l UInt8', 1, 2) AS l_tbl ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
) WHERE explain LIKE '%Strictness%';
SELECT extract(explain, 'Join conditions:.*') FROM (
    EXPLAIN SELECT * FROM values('l UInt8', 1, 2) AS l_tbl ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
) WHERE explain LIKE '%Join conditions%';

SELECT '-- ANY INNER JOIN ON constant false: empty result';
SELECT * FROM values('l UInt8', 1, 2) AS l_tbl ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 0 ORDER BY ALL;

SELECT '-- ANY LEFT / SEMI / ANTI on constant condition';
SELECT * FROM values('l UInt8', 1, 2) AS l_tbl ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1 ORDER BY ALL;
SELECT * FROM values('l UInt8', 1, 2) AS l_tbl SEMI LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1 ORDER BY ALL;
SELECT * FROM values('l UInt8', 1, 2) AS l_tbl ANTI LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1 ORDER BY ALL;
SELECT * FROM values('l UInt8', 1, 2) AS l_tbl ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1 ORDER BY ALL;

SELECT '-- ALL INNER JOIN ON constant true is still a cartesian product';
SELECT * FROM values('l UInt8', 1, 2) AS l_tbl INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1 ORDER BY ALL;

-- Compositions of ANY joins with the join graph flattening machinery: an ANY join must
-- neither absorb child joins into its query graph nor be absorbed into a parent one,
-- otherwise the original strictness would be re-applied to reconstructed child joins.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (c Int32) ENGINE = MergeTree ORDER BY c;
CREATE TABLE t2 (c Int32) ENGINE = MergeTree ORDER BY c;
CREATE TABLE t3 (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (2), (3);
INSERT INTO t3 VALUES (7), (8);

SELECT '-- outer join child under ANY INNER ON 1';
SELECT * FROM (SELECT t1.c AS a, t2.c AS b FROM t1 RIGHT JOIN t2 ON t1.c = t2.c) AS sq ANY INNER JOIN t3 ON 1 ORDER BY ALL;

SELECT '-- comma join child under ANY INNER ON 1';
SELECT * FROM (SELECT t2.c AS b, t3.c AS d FROM t2, t3) AS sq ANY INNER JOIN t1 ON 1 ORDER BY ALL;

SELECT '-- ANY INNER ON 1 child under comma join';
SELECT * FROM (SELECT t1.c AS a, t2.c AS b FROM t1 ANY INNER JOIN t2 ON 1) AS sq, t3 ORDER BY ALL;

SELECT '-- chained: ALL INNER then ANY INNER ON 1';
SELECT * FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1 ORDER BY ALL;

SELECT '-- chained: ANY INNER ON 1 then ALL INNER';
SELECT * FROM t1 ANY INNER JOIN t2 ON 1 INNER JOIN t3 ON t3.c = t3.c ORDER BY ALL;

SELECT '-- ANY LEFT ON 1 over comma join child';
SELECT * FROM (SELECT t2.c AS b, t3.c AS d FROM t2, t3) AS sq ANY LEFT JOIN t1 ON 1 ORDER BY ALL;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
