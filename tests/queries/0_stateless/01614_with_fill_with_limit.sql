SELECT
    toFloat32(number % 10) AS n,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY n ASC WITH FILL STEP 1
LIMIT 2;

SELECT
    toFloat32(number % 10) AS n,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY n ASC WITH FILL STEP 1
LIMIT 2 WITH TIES;
