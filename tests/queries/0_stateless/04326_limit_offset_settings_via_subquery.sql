-- The `limit` / `offset` settings are applied by wrapping the query as a derived table with an outer
-- `LIMIT`/`OFFSET` (see `applyQueryConstructionSettings`). This makes the cap apply to the final
-- result — in particular to the combined result of a `UNION`, not to each arm separately — and lets
-- the SQL `LIMIT` grammar handle negative (tail) and fractional (share) values natively.

-- { echoOn }
SET enable_analyzer = 1;

-- UNION: the cap applies to the whole result (3 rows), not per-arm (which would yield 6).
SELECT number FROM numbers(5) UNION ALL SELECT number + 10 FROM numbers(5) ORDER BY number SETTINGS limit = 3;

SET enable_analyzer = 0;
SELECT number FROM numbers(5) UNION ALL SELECT number + 10 FROM numbers(5) ORDER BY number SETTINGS limit = 3;

SET enable_analyzer = 1;

-- A negative `limit` selects from the tail of the final result.
SELECT number FROM numbers(10) ORDER BY number SETTINGS limit = -3;

-- A fractional `limit` in (0, 1) selects that share of the final result.
SELECT number FROM numbers(10) ORDER BY number SETTINGS limit = 0.5;

-- The `offset` setting skips rows of the final result.
SELECT number FROM numbers(10) ORDER BY number SETTINGS offset = 7;

-- `limit` and `offset` together.
SELECT number FROM numbers(10) ORDER BY number SETTINGS limit = 3, offset = 2;

-- A `limit` / `offset` carried by a subquery's own `SETTINGS` clause is applied by wrapping that
-- subquery too, so it works identically with and without the analyzer (it used to only work with
-- the analyzer), and supports negative / fractional values.
SET enable_analyzer = 1;
SELECT count() FROM (SELECT * FROM numbers(10) SETTINGS limit = 5);
SET enable_analyzer = 0;
SELECT count() FROM (SELECT * FROM numbers(10) SETTINGS limit = 5);
SET enable_analyzer = 1;
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 8 SETTINGS limit = 5);
SELECT count() FROM (SELECT * FROM numbers(10) ORDER BY number SETTINGS limit = -3);
SELECT count() FROM (SELECT * FROM numbers(10) SETTINGS limit = 0.5);
SELECT count() FROM view(SELECT * FROM numbers(10) SETTINGS limit = 5);
-- { echoOff }
