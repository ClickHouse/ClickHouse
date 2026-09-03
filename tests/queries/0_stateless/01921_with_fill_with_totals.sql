SELECT
    number,
    sum(number)
FROM numbers(10)
WHERE number % 3 = 1
GROUP BY number
    WITH TOTALS
ORDER BY number DESC WITH FILL FROM 15;

SET enable_positional_arguments = 0;

SELECT
    number,
    sum(number)
FROM numbers(10)
WHERE number % 3 = 1
GROUP BY number
    WITH TOTALS
ORDER BY 10, number DESC WITH FILL FROM 15;
