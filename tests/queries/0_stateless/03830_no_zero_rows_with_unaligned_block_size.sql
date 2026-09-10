-- Tags: stateful
-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/96574
-- When `adjustLastGranule` uses `total_rows_per_granule` instead of `numReadRows`
-- for `num_rows`, blocks could contain extra zero-filled rows (e.g. 1970-01-01 for Date columns).
-- Local tables don't reproduce the bug, so we use the stateful `test.hits` table.

SELECT
    EventDate,
    count()
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
SETTINGS max_block_size = 8482, max_threads = 2;
