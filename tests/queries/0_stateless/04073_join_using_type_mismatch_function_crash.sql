-- Regression test for "Unexpected return type from round" crash (STID: 3262-40c8)
-- When USING column has different types on each side and a function expression
-- is pushed into the join condition, the function node's result_type could become
-- stale after DAG extraction, causing a type mismatch crash during header evaluation.
-- See: https://github.com/ClickHouse/ClickHouse/issues/96101

DROP TABLE IF EXISTS t1_04073;
DROP TABLE IF EXISTS t2_04073;

CREATE TABLE t1_04073 (x UInt32, y UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2_04073 (y UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1_04073 VALUES (1, 1), (2, 2);
INSERT INTO t2_04073 VALUES (1), (2);

-- This query should not crash the server. The WHERE clause pushes round() into
-- the join condition as a key expression. With different USING column types
-- (UInt16 vs UInt64), the function's result_type must be properly re-resolved.
SELECT count() FROM t1_04073 AS a LEFT JOIN t2_04073 AS b USING (y) WHERE round(a.y) = 1;

-- Same pattern with inline subqueries (original CI-failing query shape)
SELECT 1 FROM (SELECT 1 x, 1 y) a JOIN (SELECT 1 y) b USING (y) WHERE round(*) = b.y;

DROP TABLE t1_04073;
DROP TABLE t2_04073;
