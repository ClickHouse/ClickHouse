-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/110188
-- Companion to 04321 (which covers the `arrayJoin` case).
--
-- `numbersLikeUtils::shouldPushdownLimit` rejected pushing the outer `LIMIT` into a
-- numbers source when the SELECT list contained a stateful function, but it did not
-- inspect the `WHERE`/`PREWHERE` filter. A stateful function used only in a filter
-- (e.g. `WHERE neighbor(number, 1) >= 5`) therefore let the source be capped to
-- `limit + offset` rows, so the stateful function saw a truncated input and produced
-- wrong results (here, no output at all).
--
-- `neighbor(number, 1)` shifts the column up by one within a block, so on the full
-- single-block source the filter keeps numbers 4..98 and the `LIMIT 3` yields 4, 5, 6.
-- A wrongly pushed-down `LIMIT 3` would generate only 0, 1, 2, whose neighbors are
-- 1, 2, 0 (< 5), dropping every output row. `max_block_size` is pinned so `neighbor`
-- observes one block regardless of randomized settings (`numbers(N)` is single-stream).

SET allow_deprecated_error_prone_window_functions = 1;

SELECT '-- neighbor in WHERE only';
SELECT number FROM numbers(100) WHERE neighbor(number, 1) >= 5 LIMIT 3
SETTINGS allow_experimental_analyzer = 1, max_block_size = 1000, max_threads = 1;
SELECT '-- neighbor in WHERE only, old analyzer';
SELECT number FROM numbers(100) WHERE neighbor(number, 1) >= 5 LIMIT 3
SETTINGS allow_experimental_analyzer = 0, max_block_size = 1000, max_threads = 1;
SELECT '-- neighbor in WHERE only, optimizer disabled';
SELECT number FROM numbers(100) WHERE neighbor(number, 1) >= 5 LIMIT 3
SETTINGS max_block_size = 1000, max_threads = 1, query_plan_enable_optimizations = 0;
