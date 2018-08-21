SELECT
    domain,
    hits,
    visits
FROM
(
    SELECT
        domain(URL) AS domain,
        count() AS hits
    FROM test.hits
    WHERE CounterID = 731962
    GROUP BY domain
) ANY LEFT JOIN
(
    SELECT
        domain(StartURL) AS domain,
        sum(Sign) AS visits
    FROM test.visits
    WHERE CounterID = 731962
    GROUP BY domain
) USING domain
ORDER BY hits DESC
LIMIT 10
