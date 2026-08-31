-- https://github.com/ClickHouse/ClickHouse/issues/116912
-- `optimize_trivial_group_by_limit_query` must not silently drop a `max_rows_to_group_by` cap whose
-- overflow mode throws or breaks. The default mode, `throw`, carries the same contract as an
-- explicitly set one.

DROP TABLE IF EXISTS t_trivial_group_by_limit;
CREATE TABLE t_trivial_group_by_limit (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_trivial_group_by_limit SELECT number % 100 FROM numbers(1000);

SET optimize_trivial_group_by_limit_query = 1;
SET max_rows_to_group_by = 10;

-- The mode is left at its default, `throw`.
SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 3; -- { serverError TOO_MANY_ROWS }
SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 3 SETTINGS optimize_trivial_group_by_limit_query = 0; -- { serverError TOO_MANY_ROWS }
-- Setting the mode explicitly to the same value must not change the result.
SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 3 SETTINGS group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

SELECT 'break';
SELECT count() <= 10 FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 3 SETTINGS group_by_overflow_mode = 'break');

SELECT 'any';
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 3 SETTINGS group_by_overflow_mode = 'any');

-- Without a user cap the optimization still applies.
SELECT 'no user cap';
SET max_rows_to_group_by = 0;
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 3);
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 3 SETTINGS optimize_trivial_group_by_limit_query = 0);

DROP TABLE t_trivial_group_by_limit;
