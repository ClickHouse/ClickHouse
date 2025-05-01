SELECT grouping(key_a), grouping(key_b), key_a, key_b, count()  FROM (
    SELECT 'value' as key_a, number as key_b FROM numbers(4)
)
GROUP BY GROUPING SETS((key_b), (key_a, key_b))
ORDER BY (grouping(key_a), grouping(key_b), key_a, key_b);

SELECT '=================================================';

SELECT grouping(key_a), grouping(key_b), key_a, key_b, count() FROM (
    SELECT materialize('value') as key_a, number as key_b FROM numbers(4)
)
GROUP BY GROUPING SETS((key_b), (key_a, key_b))
ORDER BY (grouping(key_a), grouping(key_b), key_a, key_b);

SELECT '=================================================';

SELECT grouping(key_a), grouping(key_b), key_a, key_b, count() FROM (
    SELECT 'value' as key_a, number as key_b FROM numbers(4)
)
GROUP BY GROUPING SETS((key_b), (key_a, key_b))
ORDER BY (grouping(key_a), grouping(key_b), key_a, key_b)
SETTINGS allow_experimental_analyzer=0;
