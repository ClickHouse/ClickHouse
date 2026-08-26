-- Tags: no-old-analyzer

-- Join conditions that are expressions (`ifNull` over a nullable range end), an SCD2-style
-- range aggregation, and both orders of the two range conditions.
-- Each result is compared with the cross join with a filter (`join_algorithm` without `ie_join`).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS calendar_scd2;
DROP TABLE IF EXISTS scd2;
DROP TABLE IF EXISTS scd2_non_null;

CREATE TABLE calendar_scd2 ENGINE = MergeTree ORDER BY tuple() AS
SELECT toDate('2022-01-01') + toIntervalMonth(number) AS dt FROM numbers(25);

CREATE TABLE scd2 ENGINE = MergeTree ORDER BY tuple() AS
SELECT dt AS range_start,
       if(toYear(dt) < 2023, toNullable(dt + toIntervalMonth(4) - toIntervalDay(1)), NULL) AS range_end,
       n
FROM calendar_scd2 CROSS JOIN (SELECT number + 1 AS n FROM numbers(85)) AS series;

CREATE TABLE scd2_non_null ENGINE = MergeTree ORDER BY tuple() AS
SELECT dt AS range_start,
       if(toYear(dt) < 2023, dt + toIntervalMonth(4) - toIntervalDay(1), toDate('2099-01-01')) AS range_end,
       n
FROM calendar_scd2 CROSS JOIN (SELECT number + 1 AS n FROM numbers(85)) AS series;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT dt, count() FROM scd2_non_null JOIN calendar_scd2 ON dt BETWEEN range_start AND ifNull(range_end, toDate('2099-01-01')) GROUP BY dt
) WHERE explain LIKE '%IEJoin%';

SELECT (
    SELECT groupArray((dt, cnt)) FROM (SELECT dt, count() AS cnt FROM scd2_non_null JOIN calendar_scd2 ON dt BETWEEN range_start AND ifNull(range_end, toDate('2099-01-01')) GROUP BY dt ORDER BY dt)
) = (
    SELECT groupArray((dt, cnt)) FROM (SELECT dt, count() AS cnt FROM scd2_non_null JOIN calendar_scd2 ON dt BETWEEN range_start AND ifNull(range_end, toDate('2099-01-01')) GROUP BY dt ORDER BY dt)
    SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

-- First key an expression over a nullable column
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT dt, count() FROM scd2 JOIN calendar_scd2 ON dt <= ifNull(range_end, toDate('2099-01-01')) AND range_start <= dt GROUP BY dt
) WHERE explain LIKE '%IEJoin%';
SELECT (
    SELECT groupArray((dt, cnt)) FROM (SELECT dt, count() AS cnt FROM scd2 JOIN calendar_scd2 ON dt <= ifNull(range_end, toDate('2099-01-01')) AND range_start <= dt GROUP BY dt ORDER BY dt)
) = (
    SELECT groupArray((dt, cnt)) FROM (SELECT dt, count() AS cnt FROM scd2 JOIN calendar_scd2 ON dt <= ifNull(range_end, toDate('2099-01-01')) AND range_start <= dt GROUP BY dt ORDER BY dt)
    SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

-- Second key an expression over a nullable column
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT dt, count() FROM scd2 JOIN calendar_scd2 ON dt BETWEEN range_start AND ifNull(range_end, toDate('2099-01-01')) GROUP BY dt
) WHERE explain LIKE '%IEJoin%';
SELECT (
    SELECT groupArray((dt, cnt)) FROM (SELECT dt, count() AS cnt FROM scd2 JOIN calendar_scd2 ON dt BETWEEN range_start AND ifNull(range_end, toDate('2099-01-01')) GROUP BY dt ORDER BY dt)
) = (
    SELECT groupArray((dt, cnt)) FROM (SELECT dt, count() AS cnt FROM scd2 JOIN calendar_scd2 ON dt BETWEEN range_start AND ifNull(range_end, toDate('2099-01-01')) GROUP BY dt ORDER BY dt)
    SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

-- Aggregate sanity check of the joined result
SELECT sum(cnt), count() FROM (SELECT dt, count() AS cnt FROM scd2 JOIN calendar_scd2 ON dt BETWEEN range_start AND ifNull(range_end, toDate('2099-01-01')) GROUP BY dt);

DROP TABLE calendar_scd2;
DROP TABLE scd2;
DROP TABLE scd2_non_null;
