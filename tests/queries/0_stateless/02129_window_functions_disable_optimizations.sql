SET optimize_rewrite_sum_if_to_count_if = 1;

SELECT if(number % 10 = 0, 1, 0) AS dummy,
sum(dummy) OVER w
FROM numbers(10)
WINDOW w AS (ORDER BY number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

SET optimize_arithmetic_operations_in_aggregate_functions=1;
SELECT
    *,
    if((number % 2) = 0, 0.5, 1) AS a,
    30 AS b,
    sum(a * b) OVER (ORDER BY number ASC) AS s
FROM numbers(10);

SET optimize_aggregators_of_group_by_keys=1;

SELECT
    *,
    if(number = 1, 1, 0) as a,
    max(a) OVER (ORDER BY number ASC) AS s
FROM numbers(10);

SET optimize_group_by_function_keys = 1;
SELECT round(sum(log(2) * number), 6) AS k FROM numbers(10000)
GROUP BY (number % 2) * (number % 3), number % 3, number % 2
HAVING sum(log(2) * number) > 346.57353 ORDER BY k;

SELECT round(sum(log(2) * number), 6) AS k FROM numbers(10000)
GROUP BY (number % 2) * (number % 3), number % 3, number % 2
HAVING sum(log(2) * number) > 346.57353 ORDER BY k
SETTINGS enable_analyzer=1;

-- An in-place rewrite of a window function's aggregate must keep it a window function.
-- https://github.com/ClickHouse/ClickHouse/issues/119635
DROP TABLE IF EXISTS t_window_subcolumn;
CREATE TABLE t_window_subcolumn (key UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_window_subcolumn VALUES (1, 5), (2, NULL), (3, 7);

SELECT count(n) OVER () AS c FROM t_window_subcolumn ORDER BY ALL SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;
SELECT count(n) OVER () AS c FROM t_window_subcolumn ORDER BY ALL SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 0;
SELECT count(n) OVER (PARTITION BY key) AS c FROM t_window_subcolumn ORDER BY ALL SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;
SELECT count(n) OVER (PARTITION BY key) AS c FROM t_window_subcolumn ORDER BY ALL SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 0;

-- The rewrite still fires, and the rewritten node is still a window function.
SELECT countIf(explain ILIKE '%function_name: sum, function_type: window%'),
       countIf(explain ILIKE '%function_type: aggregate%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(n) OVER () FROM t_window_subcolumn)
SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;

SELECT uniq(n) OVER () AS u FROM (SELECT DISTINCT n FROM t_window_subcolumn) ORDER BY ALL
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 0, optimize_uniq_to_count = 1;
SELECT uniq(n) OVER () AS u FROM (SELECT DISTINCT n FROM t_window_subcolumn) ORDER BY ALL
    SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 0, optimize_uniq_to_count = 0;

-- The uniq rewrite still fires, and the rewritten node is still a window function.
SELECT countIf(explain ILIKE '%function_name: count, function_type: window%'),
       countIf(explain ILIKE '%function_name: uniq%')
FROM (EXPLAIN QUERY TREE run_passes = 1
      SELECT uniq(n) OVER () FROM (SELECT DISTINCT n FROM t_window_subcolumn))
SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 0, optimize_uniq_to_count = 1;

DROP TABLE t_window_subcolumn;
