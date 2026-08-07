SELECT toStartOfHour(time) AS timex, id, count()
FROM
(
    SELECT
        concat('id', toString(number % 3)) AS id,
        toDateTime('2020-01-01') + (number * 60) AS time
    FROM numbers(100)
)
GROUP BY
    GROUPING SETS ( (timex, id), (timex))
ORDER BY timex ASC, id;
