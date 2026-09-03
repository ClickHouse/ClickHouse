SET enable_analyzer = 1;
SELECT
    count(),
    number AS k
FROM remote('127.0.0.{1,2}', numbers(10))
GROUP BY GROUPING SETS ((k), (k, k))
ORDER BY k;

SELECT '---';

SET enable_analyzer = 0;
SELECT
    count(),
    number AS k
FROM remote('127.0.0.{1,2}', numbers(10))
GROUP BY GROUPING SETS ((k), (k, k))
ORDER BY k;
