-- Regression tests for source/distributed pushdown guards with LIMIT AFTER/UNTIL.

-- { echo }

-- 1. Legacy WITH TOTALS + SETTINGS limit: the outer settings LimitStep must not
--    close the pipeline before LimitRangeTransform finishes draining for totals.
SELECT g, c FROM (SELECT number % 3 AS g, count() AS c FROM numbers(12) GROUP BY g WITH TOTALS ORDER BY g) LIMIT AFTER g >= 1 SETTINGS limit = 1, enable_analyzer = 0;
SELECT g, c FROM (SELECT number % 3 AS g, count() AS c FROM numbers(12) GROUP BY g WITH TOTALS ORDER BY g) LIMIT AFTER g >= 1 SETTINGS limit = 1, enable_analyzer = 1;

-- 2. No-count LIMIT AFTER must not be treated as trivial for INSERT SELECT
--    distributed execution. Verify the query at least parses and runs correctly
--    on a single server (the distributed gate is a compile-time check).
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number >= 7 SETTINGS enable_analyzer = 0;
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number >= 7 SETTINGS enable_analyzer = 1;

-- 3. LIMIT UNTIL without count must also work correctly.
SELECT number FROM numbers(10) ORDER BY number LIMIT UNTIL number >= 3 SETTINGS enable_analyzer = 0;
SELECT number FROM numbers(10) ORDER BY number LIMIT UNTIL number >= 3 SETTINGS enable_analyzer = 1;
