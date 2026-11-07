-- https://github.com/ClickHouse/ClickHouse/issues/120511
-- A function of an earlier ORDER BY column may be dropped from the sort key only while the column
-- determines the function. CUBE, ROLLUP and GROUPING SETS add rows where a key holds its default
-- value independently of the other keys, so the function has to stay in ORDER BY.

-- clickhouse-test randomizes it, and the bug only shows with it enabled.
SET optimize_redundant_functions_in_order_by = 1;

DROP TABLE IF EXISTS t_order_by_modifiers;
CREATE TABLE t_order_by_modifiers (a Int32) ENGINE = Memory;
INSERT INTO t_order_by_modifiers SELECT number - 3 FROM numbers(7);

SELECT 'CUBE';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY a, abs(a);
SELECT 'ROLLUP';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH ROLLUP ORDER BY a, abs(a);
SELECT 'GROUPING SETS';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY GROUPING SETS ((a, abs(a)), (abs(a))) ORDER BY a, abs(a);
SELECT 'CUBE, LIMIT 1';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY a, abs(a) LIMIT 1;

DROP TABLE t_order_by_modifiers;
