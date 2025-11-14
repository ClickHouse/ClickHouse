SELECT
    concat('a_', toString(number % 3)) AS a,
    number % 5 AS b
FROM numbers(50)
GROUP BY (a, b)
ORDER BY (a, b)
LIMIT 1
SETTINGS optimize_injective_functions_in_group_by = 0, enable_analyzer = 0;
