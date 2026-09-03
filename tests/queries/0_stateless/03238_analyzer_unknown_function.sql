SELECT source.count AS count
FROM
(
    SELECT
        count(*) AS count,
        key
    FROM numbers(10)
    GROUP BY number % 2 AS key
) AS source
RIGHT JOIN system.one AS r ON source.key = r.dummy;
