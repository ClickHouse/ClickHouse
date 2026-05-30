-- The optimization rewrites `SELECT agg() FROM t GROUP BY k LIMIT n`
-- into the same query with `max_rows_to_group_by = n` and
-- `group_by_overflow_mode = 'any'`, so the aggregation stops as soon as
-- `n` distinct keys are produced instead of grouping the full input.

DROP TABLE IF EXISTS t_trivial_group_by_limit;

-- Data size is kept small (10000 rows) to fit the time budget on slow storage
-- backends (e.g. `s3 storage, meta in keeper` flaky check that reruns the test
-- many times in sequence). The test asserts behavior, not data size — every
-- query below either uses LIMIT independently of input size or relies on the
-- input having at least 6 distinct keys.
CREATE TABLE t_trivial_group_by_limit (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_trivial_group_by_limit SELECT number FROM numbers(10000);

-- With the optimization on or off, the projection always returns exactly LIMIT rows,
-- so this query reports the same value either way; the difference is internal — the
-- optimization stops the aggregation once 5 distinct keys are produced.
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 0;

-- Optimization should not fire when HAVING / ORDER BY are present.
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k HAVING k > 0 LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k ORDER BY k LIMIT 5) SETTINGS optimize_trivial_group_by_limit_query = 1;

-- Optimization is suppressed when the user has explicitly set `group_by_overflow_mode = 'throw'`
-- (with any `max_rows_to_group_by`), because applying the optimization would silently override the
-- user's throw contract. With `max_rows_to_group_by = 1000000` and `'throw'` the aggregation
-- succeeds (data fits within the cap); with the optimization forcing `max_rows_to_group_by = 5`
-- and `'any'` it would silently truncate to 5 keys (or with `'throw'` retained it would throw).
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5)
SETTINGS optimize_trivial_group_by_limit_query = 1, max_rows_to_group_by = 1000000, group_by_overflow_mode = 'throw';

-- Optimization is also suppressed when the user has explicitly set `group_by_overflow_mode = 'break'`.
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5)
SETTINGS optimize_trivial_group_by_limit_query = 1, group_by_overflow_mode = 'break';

-- When the user has set `group_by_overflow_mode = 'any'` with a larger `max_rows_to_group_by`,
-- the optimization can tighten `max_rows_to_group_by` to LIMIT+OFFSET (their contract already
-- accepts truncation, so no behavior change for them, just earlier truncation).
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5)
SETTINGS optimize_trivial_group_by_limit_query = 1, max_rows_to_group_by = 1000000, group_by_overflow_mode = 'any';

-- When the user has explicitly set `group_by_overflow_mode = 'throw'` with a tight
-- `max_rows_to_group_by` (smaller than the LIMIT), applying the optimization would
-- silently lower `max_rows_to_group_by` and switch mode to `'any'`, suppressing the
-- expected throw. The pass must skip so the user's `'throw'` contract is preserved
-- and the query throws as expected when the data exceeds the user's cap.
SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1, max_rows_to_group_by = 3, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

-- `LIMIT 0` (or any `LIMIT + OFFSET == 0`): the pass must skip because
-- `max_rows_to_group_by = 0` means "no cap" in ClickHouse, so applying the
-- optimization would silently remove the user's explicit cap. The query also
-- returns no rows regardless of the optimization, so it buys nothing.
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(10)) GROUP BY k LIMIT 0)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(10)) GROUP BY k LIMIT 0 OFFSET 0)
SETTINGS optimize_trivial_group_by_limit_query = 1;

-- Optimization must not fire for `GROUP BY ... LIMIT n BY expr` queries: the outer `LIMIT n`
-- selects rows after `LIMIT BY` has limited per-`expr` rows, so stopping aggregation at `n`
-- distinct keys can starve `LIMIT BY` of groups and shrink the output.
SELECT count() FROM (
    SELECT k FROM (SELECT number AS k FROM numbers(100)) GROUP BY k LIMIT 10 BY (k % 2) LIMIT 5
) SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (
    SELECT k FROM (SELECT number AS k FROM numbers(100)) GROUP BY k LIMIT 10 BY (k % 2) LIMIT 5
) SETTINGS optimize_trivial_group_by_limit_query = 0;

-- Overflow guard: `limit + offset` must not silently wrap to a smaller number.
-- Without the guard, `LIMIT 18446744073709551615 OFFSET 100` would overflow `UInt64`
-- to 99, truncating aggregation to 99 distinct keys and yielding 0 instead of 156.
SELECT count() FROM (SELECT number % 256 AS k FROM numbers(1000) GROUP BY k LIMIT 18446744073709551615 OFFSET 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;

-- Negative LIMIT / OFFSET: analyzer keeps these as `Int64`, so reading them as
-- `UInt64` via `safeGet` would throw. The pass must skip the optimization
-- instead of failing the query. ClickHouse supports negative `LIMIT`/`OFFSET`
-- (they mean "take rows starting from the end"), so the queries below must
-- run successfully.
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(10)) GROUP BY k LIMIT -3)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(10)) GROUP BY k LIMIT -3)
SETTINGS optimize_trivial_group_by_limit_query = 0;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(10)) GROUP BY k LIMIT 3 OFFSET -2)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(10)) GROUP BY k LIMIT 3 OFFSET -2)
SETTINGS optimize_trivial_group_by_limit_query = 0;

-- Optimization must not fire for `GROUP BY` modifiers (`WITH ROLLUP`, `WITH CUBE`,
-- `WITH GROUPING SETS`, `WITH TOTALS`): forcing `max_rows_to_group_by` with
-- `group_by_overflow_mode = 'any'` would silently drop groups and corrupt the
-- subtotal/total rows produced by these modifiers.
-- Each of the queries below has 5 distinct keys plus the modifier's extra rows.

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH ROLLUP LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH ROLLUP LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH CUBE LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH CUBE LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY GROUPING SETS ((k), ()) LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY GROUPING SETS ((k), ()) LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH TOTALS LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 1;
SELECT count() FROM (SELECT k FROM (SELECT number AS k FROM numbers(5)) GROUP BY k WITH TOTALS LIMIT 100)
SETTINGS optimize_trivial_group_by_limit_query = 0;

-- The pass mutates the subquery's own context (`getMutableContext`), not the outer
-- query's context, so the outer query's `group_by_overflow_mode` should still observe
-- its default (`throw`) after the optimized subquery has switched it to `any` internally.
-- (`max_rows_to_group_by` is mutated together with the mode and would make for an
-- equivalent assertion, but the CI test profile overrides its default value, so this
-- test asserts on `group_by_overflow_mode` only — both leak together if leakage occurs.)
SELECT
    (SELECT count() FROM (SELECT k FROM t_trivial_group_by_limit GROUP BY k LIMIT 5)) AS inner_count,
    getSetting('group_by_overflow_mode') AS outer_group_by_overflow_mode
SETTINGS optimize_trivial_group_by_limit_query = 1;

DROP TABLE t_trivial_group_by_limit;
