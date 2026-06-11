-- The `limit` / `offset` settings are applied by wrapping the query as a derived table with an outer
-- `LIMIT`/`OFFSET` (see `applyQueryConstructionSettings` for the top-level query and
-- `wrapNestedLimitOffsetSettings` for subqueries that carry the setting in their own `SETTINGS`
-- clause). This makes the cap apply to the final result — in particular to the combined result of a
-- `UNION`, not to each arm separately — and lets the SQL `LIMIT` grammar handle negative (tail) and
-- fractional (share) values natively.
--
-- The queries below are written to be order-independent (a constant value, or `count()` of the
-- result), because wrapping does not impose an order on the outer query — only the size of the
-- result is well-defined.

-- { echoOn }
SET enable_analyzer = 1;

-- Top-level query: the `limit` setting caps the result; negative is the tail, a fraction is a share.
SELECT 7 FROM numbers(10) SETTINGS limit = 3;
SELECT 7 FROM numbers(10) SETTINGS limit = -3;
SELECT 7 FROM numbers(10) SETTINGS limit = 0.5;
SELECT 7 FROM numbers(10) SETTINGS offset = 7;
SELECT 7 FROM numbers(10) SETTINGS limit = 3, offset = 2;

-- Top-level UNION: the cap applies to the whole result (3 rows), not per-arm (which would yield 6).
SELECT 7 FROM numbers(5) UNION ALL SELECT 7 FROM numbers(5) SETTINGS limit = 3;
SET enable_analyzer = 0;
SELECT 7 FROM numbers(5) UNION ALL SELECT 7 FROM numbers(5) SETTINGS limit = 3;
SET enable_analyzer = 1;

-- A `limit` / `offset` carried by a subquery's own `SETTINGS` clause is applied by wrapping that
-- subquery too, so it works identically with and without the analyzer (it used to only work with
-- the analyzer), supports negative / fractional values, and combines with an explicit `LIMIT` via
-- the optimizer's push-down.
SELECT count() FROM (SELECT * FROM numbers(10) SETTINGS limit = 5);
SET enable_analyzer = 0;
SELECT count() FROM (SELECT * FROM numbers(10) SETTINGS limit = 5);
SET enable_analyzer = 1;
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 8 SETTINGS limit = 5);
SELECT count() FROM (SELECT * FROM numbers(10) SETTINGS limit = -3);
SELECT count() FROM (SELECT * FROM numbers(10) SETTINGS limit = 0.5);
SELECT count() FROM view(SELECT * FROM numbers(10) SETTINGS limit = 5);
-- { echoOff }
