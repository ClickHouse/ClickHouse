-- Tags: long
-- { echo }
-- Test for the cume_dist window function.

SET enable_analyzer = 1;

SELECT '-- Basic functionality';
SELECT number, cume_dist() OVER (ORDER BY number) FROM numbers(5);

SELECT '-- With PARTITION BY';
SELECT number, intDiv(number, 3) AS p, cume_dist() OVER (PARTITION BY p ORDER BY number) FROM numbers(10);

SELECT '-- With DESC order';
SELECT number, cume_dist() OVER (ORDER BY number DESC) FROM numbers(5);

SELECT '-- With ties in ordering values';
CREATE TABLE IF NOT EXISTS test_cume_dist (a Int, b Nullable(Int)) ENGINE = Memory;
TRUNCATE TABLE test_cume_dist;
INSERT INTO test_cume_dist VALUES (1, 10), (2, 20), (3, 20), (4, 30), (5, 30), (6, 30), (7, 40), (8, 50);
SELECT a, b, cume_dist() OVER (ORDER BY b) FROM test_cume_dist ORDER BY b, a;

SELECT '-- With multiple partitions and ties';
TRUNCATE TABLE test_cume_dist;
INSERT INTO test_cume_dist VALUES (1, 10), (1, 20), (1, 20), (2, 10), (2, 10), (2, 20);
SELECT a, b, cume_dist() OVER (PARTITION BY a ORDER BY b) FROM test_cume_dist ORDER BY a, b;

SELECT '-- With NULL values in ordering column';
TRUNCATE TABLE test_cume_dist;
INSERT INTO test_cume_dist VALUES (1, 10), (2, 20), (3, NULL), (4, 30), (5, NULL);
SELECT a, b, cume_dist() OVER (ORDER BY b) FROM test_cume_dist ORDER BY a;
SELECT a, b, cume_dist() OVER (ORDER BY b NULLS FIRST) FROM test_cume_dist ORDER BY a;
SELECT a, b, cume_dist() OVER (ORDER BY b NULLS LAST) FROM test_cume_dist ORDER BY a;

SELECT '-- Single-row partition';
SELECT a, b, cume_dist() OVER (PARTITION BY a ORDER BY b) FROM test_cume_dist WHERE a = 3;

SELECT '-- Empty partition';
SELECT a, b, cume_dist() OVER (PARTITION BY a ORDER BY b) FROM test_cume_dist WHERE a = 7;

SELECT '-- Frame specification should produce an error';
SELECT number, cume_dist() OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

DROP TABLE test_cume_dist;
