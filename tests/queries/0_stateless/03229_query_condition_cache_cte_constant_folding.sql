-- Tags: no-random-settings

-- Test for query condition cache correctness with CTE constant folding.
-- When constants are folded from CTE expressions, different constant values must produce
-- different hashes. Otherwise the query condition cache returns wrong results.
-- https://github.com/ClickHouse/ClickHouse/issues/96060

SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (activity_year Int16) ENGINE = MergeTree ORDER BY activity_year;
-- Need enough rows to have multiple granules so the cache can incorrectly exclude some.
INSERT INTO tab SELECT number % 10 + 2018 FROM numbers(100000);

SYSTEM CLEAR QUERY CONDITION CACHE;

-- First query: addMonths('2022-12-01', 0) -> year = 2022, filter: year IN (2021, 2022)
WITH block_0 AS (
    SELECT *, addMonths('2022-12-01'::date, 0) AS report_month
    FROM tab
)
SELECT count(), min(activity_year), max(activity_year) FROM block_0
WHERE (activity_year = toYear(report_month)) OR (activity_year = toYear(report_month) - 1);

-- Second query: addMonths('2022-12-01', -12) -> year = 2021, filter: year IN (2020, 2021)
-- Without the fix, this would return wrong results due to cache hash collision.
WITH block_0 AS (
    SELECT *, addMonths('2022-12-01'::date, -12) AS report_month
    FROM tab
)
SELECT count(), min(activity_year), max(activity_year) FROM block_0
WHERE (activity_year = toYear(report_month)) OR (activity_year = toYear(report_month) - 1);

-- Third query: addMonths('2022-12-01', -36) -> year = 2019, filter: year IN (2018, 2019)
WITH block_0 AS (
    SELECT *, addMonths('2022-12-01'::date, -36) AS report_month
    FROM tab
)
SELECT count(), min(activity_year), max(activity_year) FROM block_0
WHERE (activity_year = toYear(report_month)) OR (activity_year = toYear(report_month) - 1);

DROP TABLE tab;
