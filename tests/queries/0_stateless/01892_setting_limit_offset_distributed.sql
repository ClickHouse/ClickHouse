SELECT 'limit', * FROM remote('127.1', view(SELECT * FROM numbers(10))) SETTINGS limit=5;
SELECT 'offset', * FROM remote('127.1', view(SELECT * FROM numbers(10))) SETTINGS offset=5;

SELECT
    'limit w/ GROUP BY',
    count(),
    number
FROM remote('127.{1,2}', view(
    SELECT intDiv(number, 2) AS number
    FROM numbers(10)
))
GROUP BY number
ORDER BY
    count() ASC,
    number DESC
SETTINGS limit=2;

SELECT
    'limit/offset w/ GROUP BY',
    count(),
    number
FROM remote('127.{1,2}', view(
    SELECT intDiv(number, 2) AS number
    FROM numbers(10)
))
GROUP BY number
ORDER BY
    count() ASC,
    number DESC
SETTINGS limit=2, offset=2;
