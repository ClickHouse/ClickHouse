-- Regression tests for optimizer interactions with LIMIT AFTER/UNTIL.
-- Each test targets a specific optimizer pass that could break range semantics.

-- { echo }

-- 1. Source-level limit pushdown: numbers() must not cap rows before LimitRangeStep.
--    Without the guard, numbers(10) LIMIT 3 generates only 3 rows, and AFTER number >= 8
--    finds nothing.
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 8;
SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 8 SETTINGS enable_analyzer = 0;

-- 2. Redundant sort removal: outer ORDER BY must not remove inner ORDER BY that feeds
--    the AFTER boundary. Without the guard the inner sort is removed and AFTER sees
--    unsorted data.
SELECT * FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5) ORDER BY number DESC;
SELECT * FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT 3 AFTER number >= 5) ORDER BY number DESC SETTINGS enable_analyzer = 0;

-- 3. Trivial GROUP BY + LIMIT n AFTER: the optimization must not set max_rows_to_group_by = n
--    which would stop aggregation before the boundary key appears.
SELECT number % 100 AS k FROM numbers(10000) GROUP BY k LIMIT 5 AFTER k >= 50 SETTINGS optimize_trivial_group_by_limit_query = 1;

-- 4. WITH TOTALS + SETTINGS limit: the outer settings LimitStep must propagate
--    always_read_till_end so totals are not lost.
SELECT number % 3 AS g, count() FROM numbers(12) GROUP BY g WITH TOTALS ORDER BY g LIMIT AFTER g >= 1 SETTINGS limit = 1;

-- 5. Predicate pushdown barrier: outer WHERE must not be pushed into a subquery with
--    LIMIT AFTER (legacy path). A pushed predicate could remove the boundary row.
SELECT * FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number >= 3) WHERE number <= 5 SETTINGS enable_analyzer = 0;
