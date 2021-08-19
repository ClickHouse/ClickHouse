SELECT
    toDate((number * 10) * 86400) AS d1, 
    toDate(number * 86400) AS d2, 
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL, 
    d1 WITH FILL STEP 5;

SELECT '===============';

SELECT
    toDate((number * 10) * 86400) AS d1, 
    toDate(number * 86400) AS d2, 
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;