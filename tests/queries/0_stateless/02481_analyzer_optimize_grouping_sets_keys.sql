set enable_analyzer = 1;
set optimize_syntax_fuse_functions = 0;

EXPLAIN QUERY TREE run_passes=1
SELECT avg(log(2) * number) AS k FROM numbers(10000000)
GROUP BY GROUPING SETS (((number % 2) * (number % 3)), number % 3, number % 2)
HAVING avg(log(2) * number) > 3465735.3
ORDER BY k;

EXPLAIN QUERY TREE run_passes=1
SELECT avg(log(2) * number) AS k FROM numbers(10000000)
GROUP BY GROUPING SETS (((number % 2) * (number % 3), number % 3, number % 2), (number % 4))
HAVING avg(log(2) * number) > 3465735.3
ORDER BY k;

EXPLAIN QUERY TREE run_passes=1
SELECT avg(log(2) * number) AS k FROM numbers(10000000)
GROUP BY GROUPING SETS (((number % 2) * (number % 3), number % 3), (number % 2))
HAVING avg(log(2) * number) > 3465735.3
ORDER BY k;

EXPLAIN QUERY TREE run_passes=1
SELECT count() FROM numbers(1000)
GROUP BY GROUPING SETS
    (
        (number, number + 1, number +2),
        (number % 2, number % 3),
        (number / 2, number / 3)
    );
