--https://github.com/ClickHouse/ClickHouse/issues/57086
SELECT
    'limit w/ GROUP BY',
    count(NULL),
    number
FROM remote('127.{1,2}', view(
        SELECT intDiv(number, 2147483647) AS number
        FROM numbers(10)
    ))
GROUP BY number
WITH ROLLUP
ORDER BY
    count() ASC,
    number DESC NULLS LAST
    SETTINGS limit = 2, enable_analyzer = 1;
