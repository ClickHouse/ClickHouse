SELECT
    CounterID,
    count() AS hits,
    any(visits)
FROM test.hits ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
GROUP BY CounterID
ORDER BY hits DESC
LIMIT 10;
