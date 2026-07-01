-- Regression test for "Unexpected return type from equals. Expected Nullable(UInt8). Got UInt8"
-- when using RIGHT JOIN + LEFT JOIN with USING clause and Nullable column types.
-- The filter is pushed down to the null-supplying side of RIGHT JOIN but the function's
-- expected return type (Nullable) doesn't match the actual input column type (non-Nullable).
-- See: https://github.com/ClickHouse/ClickHouse/issues/96101

SET enable_analyzer = 1;
SET query_plan_use_new_logical_join_step = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt8, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (id UInt16, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (id Nullable(UInt64), value String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (0, 'a'), (1, 'b'), (2, 'c');
INSERT INTO t2 VALUES (0, 'x'), (1, 'y'), (3, 'z');
INSERT INTO t3 VALUES (0, 'p'), (1, 'q'), (NULL, 'r');

-- RIGHT JOIN makes t1 the null-supplying side; LEFT JOIN with Nullable t3.id
-- causes the USING result to be Nullable, and the WHERE filter must handle this correctly.
SELECT 1 FROM t1 RIGHT JOIN t2 USING (id) LEFT JOIN t3 USING (id) WHERE id = 10;

SELECT id FROM t1 RIGHT JOIN t2 USING (id) LEFT JOIN t3 USING (id) WHERE id = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
