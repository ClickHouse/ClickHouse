-- Tags: no-old-analyzer
-- The `EXPLAIN SYNTAX` below renders differently under the old analyzer, which expands `*` and pushes
-- the predicate into the subquery, so this test runs only with the (default) new analyzer. The numeric
-- assertions are analyzer-independent (the construction settings are applied by wrapping in `executeQuery`).

-- Tests the query-construction settings (`select` / `filter` / `order` / `sort` and
-- `limit` / `offset` / `page`). They shape a query's *result*, materialized by wrapping the
-- (sub)query as a derived table in `executeQuery` rather than folded into the query tree by the
-- analyzer. A `SETTINGS` clause applies to its own scope (not to deeper subqueries); session/user
-- settings apply only to the outermost query; the query explained by `EXPLAIN` is wrapped too.
-- They follow the same scope rules as any other setting on `INSERT ... SELECT` / `CREATE ... AS SELECT`:
-- a setting in the source `SELECT`'s own `SETTINGS` clause shapes it (so it is effective), while a
-- setting on the `INSERT` / `CREATE` statement itself does not propagate into the `SELECT`.

SELECT '-- page in a subquery SETTINGS is translated to offset (page 2, limit 10 -> rows 10..19)';
SELECT min(number), max(number), count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 10, page = 2);

SELECT '-- page in a subquery SETTINGS without limit is rejected (not silently dropped)';
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS page = 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- limit/offset in a subquery SETTINGS cap the subquery result';
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 5);
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 5, offset = 3);

SELECT '-- an arm-local setting on a non-last arm plus SETTINGS on the last arm is ambiguous and rejected';
SELECT count() FROM ((SELECT number FROM numbers(100) SETTINGS limit = 1) UNION ALL (SELECT number FROM numbers(100) SETTINGS limit = 2)); -- { serverError BAD_ARGUMENTS }

SELECT '-- nesting per-arm settings in subqueries applies them per arm unambiguously (1 + 2 = 3)';
SELECT count() FROM (SELECT * FROM (SELECT number FROM numbers(100) SETTINGS limit = 1) UNION ALL SELECT * FROM (SELECT number FROM numbers(100) SETTINGS limit = 2));

SELECT '-- a trailing query-level SETTINGS limit still caps the whole union (3, not 5 + 3)';
SELECT count() FROM (SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) SETTINGS limit = 3);

SELECT '-- a construction setting on the INSERT statement itself does not propagate to the SELECT (all 10 rows)';
DROP TABLE IF EXISTS t_construction_settings;
CREATE TABLE t_construction_settings (x UInt64) ENGINE = Memory;
INSERT INTO t_construction_settings SETTINGS limit = 2 SELECT number FROM numbers(10);
SELECT count() FROM t_construction_settings;

SELECT '-- but a construction setting on the source SELECT is effective (2 rows)';
TRUNCATE TABLE t_construction_settings;
INSERT INTO t_construction_settings SELECT number FROM numbers(10) SETTINGS limit = 2;
SELECT count() FROM t_construction_settings;
DROP TABLE t_construction_settings;

SELECT '-- filter in a subquery SETTINGS filters that subquery (numbers 6..9 -> 4)';
SELECT count() FROM (SELECT number FROM numbers(10) SETTINGS filter = 'number > 5');

SELECT '-- select in a subquery SETTINGS projects that subquery';
SELECT a FROM (SELECT number AS a, number * 2 AS b FROM numbers(3) SETTINGS select = 'a') ORDER BY a;

SELECT '-- order in a subquery SETTINGS orders that subquery';
SELECT groupArray(number) FROM (SELECT number FROM numbers(3) SETTINGS order = '-number');

SELECT '-- a setting on the outermost query applies only to its scope, not to the subquery (count = 10, not 3)';
SELECT count() FROM (SELECT number FROM numbers(10)) SETTINGS limit = 3;

SELECT '-- EXPLAIN wraps the explained query so its plan matches execution';
EXPLAIN SYNTAX SELECT number FROM numbers(10) SETTINGS filter = 'number > 5';

SELECT '-- a construction setting on the source SELECT of CREATE ... AS SELECT is effective (3 rows)';
DROP TABLE IF EXISTS t_create_as_select;
CREATE TABLE t_create_as_select ENGINE = Memory AS SELECT number FROM numbers(10) SETTINGS limit = 3;
SELECT count() FROM t_create_as_select;
DROP TABLE IF EXISTS t_create_as_select;

SELECT '-- a non-construction `= DEFAULT` reset on a subquery survives the construction-settings consumption (returns 10)';
-- The session caps table reads at 5 rows; the subquery resets that cap to DEFAULT alongside the
-- construction setting `limit`. Consuming `limit` empties the SETTINGS node's `changes`, but the node
-- must be kept because `default_settings` still holds the reset — so the wrapped inner `numbers(100)`
-- read is uncapped and the query returns 10. If the reset were dropped, the inherited 5-row cap would
-- throw TOO_MANY_ROWS on the read.
SET max_rows_to_read = 5;
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 10, max_rows_to_read = DEFAULT);
SET max_rows_to_read = DEFAULT;

SELECT '-- a `= DEFAULT` reset on an arm merged into the trailing query-level SETTINGS survives (returns 100)';
-- The outer (query-level) SETTINGS survives stripping (it still carries `max_threads`), so the inner arm's
-- `max_rows_to_read = DEFAULT` is merged into it rather than dropped. Under a session cap of 5 the merged
-- reset keeps the inner count() read (100 rows) uncapped; if it were dropped, that read would throw.
SET max_rows_to_read = 5;
(SELECT count() FROM numbers(100) SETTINGS max_rows_to_read = DEFAULT) SETTINGS max_threads = 1, limit = 5;
SET max_rows_to_read = DEFAULT;
