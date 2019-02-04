SELECT
    EventDate,
    hits,
    visits
FROM
(
    SELECT
        EventDate,
        count() AS hits
    FROM test.hits
    GROUP BY EventDate
) ANY LEFT JOIN
(
    SELECT
        StartDate AS EventDate,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY EventDate
) USING EventDate
ORDER BY hits DESC
LIMIT 10;
