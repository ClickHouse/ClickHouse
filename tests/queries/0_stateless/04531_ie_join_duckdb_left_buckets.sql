-- Tags: no-old-analyzer

-- Bucketing a year of daily epochs into 40 ranges with a LEFT range join and counting rows
-- per bucket: the verbatim LEFT form of the aggregation whose INNER adaptation is in 04539.
-- `join_use_nulls = 1` so `count(ts)` counts only matched rows in would-be-empty buckets
-- (all buckets are non-empty here, so the result equals the INNER one).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET join_use_nulls = 1;

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
) WHERE explain LIKE '%IEJoin%';

WITH data_table AS (SELECT toInt64(1577836800 + number * 86400) AS ts FROM numbers(367)),
S AS (SELECT min(ts) AS minVal, max(ts) - min(ts) AS spread FROM data_table),
buckets AS (
    SELECT toInt64(number) AS bucket,
        toInt64(intDiv(number * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS low,
        toInt64(intDiv((number + 1) * (SELECT spread FROM S), 40) + (SELECT minVal FROM S)) AS high
    FROM numbers(40))
SELECT bucket, low, high, count(data_table.ts) AS cnt
FROM buckets LEFT JOIN data_table ON data_table.ts >= buckets.low AND data_table.ts < buckets.high
GROUP BY bucket, low, high ORDER BY bucket;
