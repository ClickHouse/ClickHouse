-- https://github.com/ClickHouse/ClickHouse/issues/86442
-- The transformation shouldn't be applied if the aggregation has window parameters
SELECT uniqExact(c0) OVER (ORDER BY c0 DESC)
FROM
(
    SELECT number AS c0
    FROM numbers(10)
) AS t0
SETTINGS count_distinct_optimization = 1, enable_analyzer = 1;