-- https://github.com/ClickHouse/ClickHouse/issues/120511
-- A function of an earlier ORDER BY column may be dropped from the sort key only while the column
-- determines the function. CUBE, ROLLUP and GROUPING SETS add rows where a key holds its default
-- value independently of the other keys, so the function has to stay in ORDER BY.

-- Both are randomized by clickhouse-test; the query tree output below depends on them.
SET optimize_redundant_functions_in_order_by = 1;
SET optimize_group_by_function_keys = 1;

DROP TABLE IF EXISTS t_order_by_modifiers;
CREATE TABLE t_order_by_modifiers (a Int32, n Nullable(Int32)) ENGINE = Memory;
INSERT INTO t_order_by_modifiers SELECT number - 3, number - 3 FROM numbers(7);

SELECT 'CUBE';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY a, abs(a);
SELECT 'ROLLUP';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH ROLLUP ORDER BY a, abs(a);
SELECT 'GROUPING SETS';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY GROUPING SETS ((a, abs(a)), (abs(a))) ORDER BY a, abs(a);
SELECT 'CUBE, LIMIT 1';
SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY a, abs(a) LIMIT 1;

-- 1 when the result is ordered by (a, f(a)), 0 when it is not
SELECT 'ordered: CUBE', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, abs(a)) AS t FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY a, abs(a));
SELECT 'ordered: ROLLUP', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, abs(a)) AS t FROM t_order_by_modifiers GROUP BY a, abs(a) WITH ROLLUP ORDER BY a, abs(a));
SELECT 'ordered: GROUPING SETS', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, abs(a)) AS t FROM t_order_by_modifiers GROUP BY GROUPING SETS ((a, abs(a)), (abs(a))) ORDER BY a, abs(a));
SELECT 'ordered: CUBE, toString', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, toString(a)) AS t FROM t_order_by_modifiers GROUP BY a, toString(a) WITH CUBE ORDER BY a, toString(a));
SELECT 'ordered: CUBE, Nullable key', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (n, abs(n)) AS t FROM t_order_by_modifiers GROUP BY n, abs(n) WITH CUBE ORDER BY n, abs(n));
SELECT 'ordered: CUBE, alias and positional ORDER BY', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, f) AS t FROM (SELECT a, abs(a) AS f FROM t_order_by_modifiers GROUP BY a, f WITH CUBE ORDER BY 1, 2));
SELECT 'ordered: CUBE, three keys', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (intDiv(a, 2), a, abs(a)) AS t FROM t_order_by_modifiers GROUP BY intDiv(a, 2), a, abs(a) WITH CUBE ORDER BY intDiv(a, 2), a, abs(a));

-- shapes that were already ordered: the function is still dropped where the column determines it
SELECT 'ordered: plain GROUP BY', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, abs(a)) AS t FROM t_order_by_modifiers GROUP BY a, abs(a) ORDER BY a, abs(a));
SELECT 'ordered: WITH TOTALS', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, abs(a)) AS t FROM t_order_by_modifiers GROUP BY a, abs(a) WITH TOTALS ORDER BY a, abs(a));
SELECT 'ordered: CUBE, function first', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (abs(a), a) AS t FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY abs(a), a);
SELECT 'ordered: CUBE, group_by_use_nulls', groupArray(t) = arraySort(groupArray(t))
FROM (SELECT (a, abs(a)) AS t FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY a, abs(a) SETTINGS group_by_use_nulls = 1);

-- the sort key list itself: kept under a modifier, still reduced for a plain GROUP BY
SELECT 'query tree: CUBE';
EXPLAIN QUERY TREE run_passes = 1, dump_tree = 0, dump_ast = 1 SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH CUBE ORDER BY a, abs(a);
SELECT 'query tree: ROLLUP';
EXPLAIN QUERY TREE run_passes = 1, dump_tree = 0, dump_ast = 1 SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH ROLLUP ORDER BY a, abs(a);
SELECT 'query tree: GROUPING SETS';
EXPLAIN QUERY TREE run_passes = 1, dump_tree = 0, dump_ast = 1 SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY GROUPING SETS ((a, abs(a)), (abs(a))) ORDER BY a, abs(a);
SELECT 'query tree: plain GROUP BY';
EXPLAIN QUERY TREE run_passes = 1, dump_tree = 0, dump_ast = 1 SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) ORDER BY a, abs(a);
SELECT 'query tree: WITH TOTALS';
EXPLAIN QUERY TREE run_passes = 1, dump_tree = 0, dump_ast = 1 SELECT a, abs(a) FROM t_order_by_modifiers GROUP BY a, abs(a) WITH TOTALS ORDER BY a, abs(a);

DROP TABLE t_order_by_modifiers;
