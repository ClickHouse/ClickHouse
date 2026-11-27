set enable_analyzer = 1;
SET optimize_injective_functions_inside_uniq = 1;
SET optimize_injective_functions_in_group_by = 1;
SET optimize_aggregators_of_group_by_keys = 1;

EXPLAIN QUERY TREE
SELECT toString(toString(number + 1)) as val, count()
FROM numbers(2)
GROUP BY val
ORDER BY val;

SELECT toString(toString(number + 1)) as val, count()
FROM numbers(2)
GROUP BY ALL
ORDER BY val;

EXPLAIN QUERY TREE
SELECT toString(toString(number + 1)) as val, count()
FROM numbers(2)
GROUP BY ALL
ORDER BY val;

SELECT 'CHECK WITH TOTALS';

EXPLAIN QUERY TREE
SELECT toString(toString(number + 1)) as val, count()
FROM numbers(2)
GROUP BY val WITH TOTALS
ORDER BY val;

SELECT toString(toString(number + 1)) as val, count()
FROM numbers(2)
GROUP BY val WITH TOTALS
ORDER BY val;
