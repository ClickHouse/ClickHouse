-- Tags: stateful
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
    GROUP BY domain
) ANY LEFT JOIN
(
    SELECT
        domain(StartURL) AS domain,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY domain
) USING domain
ORDER BY hits DESC
LIMIT 10
SETTINGS joined_subquery_requires_alias = 0;
