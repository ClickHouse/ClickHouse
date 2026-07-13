SELECT
    number,
    countIf(1, number > 0)
FROM numbers(10)
GROUP BY number
HAVING (count() <= 10) AND 1
ORDER BY number ASC
SETTINGS enable_analyzer = 1;
