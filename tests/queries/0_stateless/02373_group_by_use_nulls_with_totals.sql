SELECT number
FROM numbers(10)
GROUP BY number
    WITH TOTALS
ORDER BY number
SETTINGS group_by_use_nulls = 1;

