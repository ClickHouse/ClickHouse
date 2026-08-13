set enable_analyzer = 1;
set optimize_move_functions_out_of_any = 0;

SELECT 'set optimize_aggregators_of_group_by_keys = 1';
set optimize_aggregators_of_group_by_keys = 1;

SELECT min(number % 2) AS a, max(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
SELECT any(number % 2) AS a, anyLast(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
SELECT max((number % 5) * (number % 7)) AS a FROM numbers(10000000) GROUP BY number % 7, number % 5 ORDER BY a;
SELECT foo FROM (SELECT anyLast(number) AS foo FROM numbers(1) GROUP BY number);
SELECT anyLast(number) FROM numbers(1) GROUP BY number;

EXPLAIN QUERY TREE SELECT min(number % 2) AS a, max(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
EXPLAIN QUERY TREE SELECT any(number % 2) AS a, anyLast(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
EXPLAIN QUERY TREE SELECT max((number % 5) * (number % 7)) AS a FROM numbers(10000000) GROUP BY number % 7, number % 5 ORDER BY a;
EXPLAIN QUERY TREE SELECT foo FROM (SELECT anyLast(number) AS foo FROM numbers(1) GROUP BY number);

EXPLAIN QUERY TREE
SELECT min(number) OVER (PARTITION BY number % 2)
FROM numbers(3)
GROUP BY number;

SELECT 'GROUPING SETS tests (optimization enabled)';
SELECT max(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a;
SELECT min(number % 2) AS a, max(number % 2) AS b FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a, b;
SELECT min(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a;
SELECT max(number % 2) AS a, min(number % 3) AS b FROM numbers(100) GROUP BY GROUPING SETS ((number % 2, number % 3), (number % 2, number % 3, number % 5)) ORDER BY a, b;
SELECT max(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 3)) ORDER BY a;
EXPLAIN QUERY TREE SELECT max(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a;
EXPLAIN QUERY TREE SELECT min(number % 2) AS a, max(number % 2) AS b FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a, b;
EXPLAIN QUERY TREE SELECT min(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a;
EXPLAIN QUERY TREE SELECT max(number % 2) AS a, min(number % 3) AS b FROM numbers(100) GROUP BY GROUPING SETS ((number % 2, number % 3), (number % 2, number % 3, number % 5)) ORDER BY a, b;
EXPLAIN QUERY TREE SELECT max(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 3)) ORDER BY a;

SELECT 'GROUPING SETS negative test: b is not in all sets, so max(b) should NOT be eliminated';
DROP TABLE IF EXISTS test_grouping_sets;
CREATE TABLE test_grouping_sets (a UInt8, b UInt8) ENGINE = Memory;
INSERT INTO test_grouping_sets SELECT
    number % 2 + 1 AS a,
    (number % 5 + 1) * 10 + (number % 2) * 50 AS b
FROM numbers(1000);
SELECT a, max(b) AS mb FROM test_grouping_sets GROUP BY GROUPING SETS ((a, b), (a)) ORDER BY a, mb;
EXPLAIN QUERY TREE SELECT a, max(b) AS mb FROM test_grouping_sets GROUP BY GROUPING SETS ((a, b), (a)) ORDER BY a, mb;
DROP TABLE test_grouping_sets;

SELECT 'set optimize_aggregators_of_group_by_keys = 0';
set optimize_aggregators_of_group_by_keys = 0;

SELECT min(number % 2) AS a, max(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
SELECT any(number % 2) AS a, anyLast(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
SELECT max((number % 5) * (number % 7)) AS a FROM numbers(10000000) GROUP BY number % 7, number % 5 ORDER BY a;
SELECT foo FROM (SELECT anyLast(number) AS foo FROM numbers(1) GROUP BY number);

EXPLAIN QUERY TREE SELECT min(number % 2) AS a, max(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
EXPLAIN QUERY TREE SELECT any(number % 2) AS a, anyLast(number % 3) AS b FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY a, b;
EXPLAIN QUERY TREE SELECT max((number % 5) * (number % 7)) AS a FROM numbers(10000000) GROUP BY number % 7, number % 5 ORDER BY a;
EXPLAIN QUERY TREE SELECT foo FROM (SELECT anyLast(number) AS foo FROM numbers(1) GROUP BY number);

EXPLAIN QUERY TREE
SELECT min(number) OVER (PARTITION BY number % 2)
FROM numbers(3)
GROUP BY number;

SELECT 'GROUPING SETS tests (optimization disabled)';
SELECT max(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a;
EXPLAIN QUERY TREE SELECT max(number % 2) AS a FROM numbers(100) GROUP BY GROUPING SETS ((number % 2), (number % 2, number % 3)) ORDER BY a;
