-- Tags: no-old-analyzer

-- Basic IEJoin scenarios: range conditions with extra inequalities, joins inside CTEs,
-- range bucketing, and an IEJoin at the end of a chain of joins.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

-- Two range conditions with two additional `<>` conditions: for INNER the range conditions
-- become the IEJoin conditions and the `<>` conditions a filter over the join result
WITH test AS (SELECT number AS id, toInt64(number) AS b, toInt64(number + 10) AS e, number % 2 AS p1, number % 3 AS p2 FROM numbers(10))
SELECT lhs.id, rhs.id
FROM test lhs JOIN test rhs ON lhs.b < rhs.e AND rhs.b < lhs.e AND lhs.p1 <> rhs.p1 AND lhs.p2 <> rhs.p2
ORDER BY ALL;

-- Subquery/CTE around the join
WITH test AS (SELECT number AS id, toInt64(number) AS b, toInt64(number + 10) AS e, number % 2 AS p1, number % 3 AS p2 FROM numbers(10)),
sub AS (
    SELECT lhs.id AS lid, rhs.id AS rid
    FROM test lhs JOIN test rhs ON lhs.b < rhs.e AND rhs.b < lhs.e AND lhs.p1 <> rhs.p1 AND lhs.p2 <> rhs.p2
)
SELECT min(lid), max(rid) FROM sub;

-- Bucketing a year of daily epochs into 40 ranges with a range join and counting rows per bucket
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    WITH data_table AS (SELECT toInt64(1577836800 + number * 86400) AS ts FROM numbers(367)),
    S AS (SELECT min(ts) AS minVal, max(ts) - min(ts) AS spread FROM data_table),
    buckets AS (
        SELECT toInt64(number) AS bucket,
            toInt64(intDiv(number * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS low,
            toInt64(intDiv((number + 1) * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS high
        FROM numbers(40))
    SELECT bucket, low, high, count() AS cnt
    FROM buckets JOIN data_table ON data_table.ts >= buckets.low AND data_table.ts < buckets.high
    GROUP BY bucket, low, high ORDER BY bucket
) WHERE explain LIKE '%IEJoin%';

WITH data_table AS (SELECT toInt64(1577836800 + number * 86400) AS ts FROM numbers(367)),
S AS (SELECT min(ts) AS minVal, max(ts) - min(ts) AS spread FROM data_table),
buckets AS (
    SELECT toInt64(number) AS bucket,
        toInt64(intDiv(number * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS low,
        toInt64(intDiv((number + 1) * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS high
    FROM numbers(40))
SELECT bucket, low, high, count() AS cnt
FROM buckets JOIN data_table ON data_table.ts >= buckets.low AND data_table.ts < buckets.high
GROUP BY bucket, low, high ORDER BY bucket;

-- The LEFT form of the same bucketing: `join_use_nulls = 1` so `count(data_table.ts)` counts
-- only matched rows in would-be-empty buckets (all buckets are non-empty here, so the result
-- must equal the INNER one row for row)
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    WITH data_table AS (SELECT toInt64(1577836800 + number * 86400) AS ts FROM numbers(367)),
    S AS (SELECT min(ts) AS minVal, max(ts) - min(ts) AS spread FROM data_table),
    buckets AS (
        SELECT toInt64(number) AS bucket,
            toInt64(intDiv(number * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS low,
            toInt64(intDiv((number + 1) * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS high
        FROM numbers(40))
    SELECT bucket, low, high, count(data_table.ts) AS cnt
    FROM buckets LEFT JOIN data_table ON data_table.ts >= buckets.low AND data_table.ts < buckets.high
    GROUP BY bucket, low, high ORDER BY bucket
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%IEJoin%';

SELECT 'left buckets equal inner', (
    SELECT groupArray((bucket, low, high, cnt)) FROM (
        WITH data_table AS (SELECT toInt64(1577836800 + number * 86400) AS ts FROM numbers(367)),
        S AS (SELECT min(ts) AS minVal, max(ts) - min(ts) AS spread FROM data_table),
        buckets AS (
            SELECT toInt64(number) AS bucket,
                toInt64(intDiv(number * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS low,
                toInt64(intDiv((number + 1) * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS high
            FROM numbers(40))
        SELECT bucket, low, high, count(data_table.ts) AS cnt
        FROM buckets LEFT JOIN data_table ON data_table.ts >= buckets.low AND data_table.ts < buckets.high
        GROUP BY bucket, low, high ORDER BY bucket
        SETTINGS join_use_nulls = 1
    )
) = (
    SELECT groupArray((bucket, low, high, cnt)) FROM (
        WITH data_table AS (SELECT toInt64(1577836800 + number * 86400) AS ts FROM numbers(367)),
        S AS (SELECT min(ts) AS minVal, max(ts) - min(ts) AS spread FROM data_table),
        buckets AS (
            SELECT toInt64(number) AS bucket,
                toInt64(intDiv(number * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS low,
                toInt64(intDiv((number + 1) * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS high
            FROM numbers(40))
        SELECT bucket, low, high, count() AS cnt
        FROM buckets JOIN data_table ON data_table.ts >= buckets.low AND data_table.ts < buckets.high
        GROUP BY bucket, low, high ORDER BY bucket
    )
);

-- A chain of an equality join (empty result), a cross join and an inequality join must not break
DROP TABLE IF EXISTS test_big;
DROP TABLE IF EXISTS test_small;
CREATE TABLE test_big ENGINE = MergeTree ORDER BY i AS SELECT toInt64(number) AS i, toInt64(number + 100000) AS j, 'hello' AS k FROM numbers(20000);
CREATE TABLE test_small ENGINE = MergeTree ORDER BY i AS SELECT toInt64(number * 10) AS i, toInt64(number * 10 + 100000) AS j, 'hello' AS k FROM numbers(2000);
SELECT count() FROM test_small t1 JOIN test_small t2 ON t1.i = t2.j CROSS JOIN test_small t3 JOIN test_big t4 ON t3.i < t4.i AND t3.j > t4.j;
DROP TABLE test_big;
DROP TABLE test_small;
