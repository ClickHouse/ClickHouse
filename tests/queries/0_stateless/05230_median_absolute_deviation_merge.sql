SELECT madMerge(state)
FROM
(
    SELECT number % 4 AS key, madState(number) AS state
    FROM numbers(100)
    GROUP BY key
);

SELECT mad(number)
FROM numbers(100);
