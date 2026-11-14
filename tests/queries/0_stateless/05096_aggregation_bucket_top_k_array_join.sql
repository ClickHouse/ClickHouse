-- https://github.com/ClickHouse/ClickHouse/issues/116827
-- `query_plan_aggregation_bucket_top_k` keeps only the best n groups of each two-level aggregation
-- bucket. An `arrayJoin` between the aggregation and the sort changes row multiplicity per group and
-- produces no row at all for an empty array, so the groups pruned inside a bucket could be precisely
-- the ones that would have survived, and the query silently returned fewer rows.

SELECT count() FROM
(
    SELECT k, c, arrayJoin(range(c % 2)) AS j
    FROM
    (
        SELECT k, count() AS c
        FROM
        (
            SELECT intDiv(number, 2) AS k FROM numbers(600000)
            UNION ALL
            SELECT 1000000 + number AS k FROM numbers(5)
        )
        GROUP BY k
    )
    ORDER BY c DESC
    LIMIT 5
);

SELECT count() FROM
(
    SELECT k, c, arrayJoin(range(c % 2)) AS j
    FROM
    (
        SELECT k, count() AS c
        FROM
        (
            SELECT intDiv(number, 2) AS k FROM numbers(600000)
            UNION ALL
            SELECT 1000000 + number AS k FROM numbers(5)
        )
        GROUP BY k
    )
    ORDER BY c DESC
    LIMIT 5
) SETTINGS query_plan_aggregation_bucket_top_k = 0;

SELECT 'the optimization still applies without an arrayJoin';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k, count() AS c FROM (SELECT intDiv(number, 2) AS k FROM numbers(600000)) GROUP BY k ORDER BY c DESC LIMIT 5
) WHERE explain LIKE '%Bucket top-K%';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT k, c, arrayJoin(range(c % 2)) AS j
    FROM (SELECT k, count() AS c FROM (SELECT intDiv(number, 2) AS k FROM numbers(600000)) GROUP BY k)
    ORDER BY c DESC LIMIT 5
) WHERE explain LIKE '%Bucket top-K%';
