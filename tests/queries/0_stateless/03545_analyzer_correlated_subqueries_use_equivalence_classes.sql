SET allow_experimental_correlated_subqueries = 1;

EXPLAIN actions = 1
SELECT 
    (SELECT
        count()
    FROM
        numbers(10)
    WHERE
        number = n.number)
FROM
    numbers(10) n;
