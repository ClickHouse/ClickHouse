set enable_analyzer = 1;
set optimize_syntax_fuse_functions = 0;
SET optimize_group_by_constant_keys = 1, optimize_group_by_function_keys = 1;
SET optimize_aggregators_of_group_by_keys = 1;
SET optimize_injective_functions_in_group_by = 1;
SET optimize_extract_common_expressions = 0;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

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
