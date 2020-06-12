SELECT dummy IN (0) AS x, count() GROUP BY x;

SELECT 1 IN (0) AS x, count() GROUP BY x;
SELECT 0 IN (0) AS x, count() GROUP BY x;
SELECT materialize(1) IN (0) AS x, count() GROUP BY x;
SELECT materialize(0) IN (0) AS x, count() GROUP BY x;

SELECT
    number IN (1, 2) AS x,
    count()
FROM numbers(10)
GROUP BY x;
