-- Tags: no-old-analyzer
-- The `EXPLAIN SYNTAX` below renders differently under the old analyzer, which expands `*` and pushes
-- the predicate into the subquery, so this test runs only with the (default) new analyzer. The numeric
-- assertions are analyzer-independent (the construction settings are applied by wrapping in `executeQuery`).

-- Tests the query-construction settings (`select` / `filter` / `order` / `sort` and
-- `limit` / `offset` / `page`). They shape a query's *result*, materialized by wrapping the
-- (sub)query as a derived table in `executeQuery` rather than folded into the query tree by the
-- analyzer. A `SETTINGS` clause applies to its own scope (not to deeper subqueries); session/user
-- settings apply only to the outermost query; the query explained by `EXPLAIN` is wrapped too.
-- Because they are result modifiers, they are irrelevant for (and not applied to) `INSERT ... SELECT`,
-- `CREATE ... AS SELECT`, and similar queries that do not return a result.

SELECT '-- page in a subquery SETTINGS is translated to offset (page 2, limit 10 -> rows 10..19)';
SELECT min(number), max(number), count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 10, page = 2);

SELECT '-- page in a subquery SETTINGS without limit is rejected (not silently dropped)';
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS page = 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- limit/offset in a subquery SETTINGS cap the subquery result';
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 5);
SELECT count() FROM (SELECT number FROM numbers(100) SETTINGS limit = 5, offset = 3);

SELECT '-- per-UNION-arm limit is applied per arm, not collapsed onto the whole union (1 + 2 = 3)';
SELECT count() FROM ((SELECT number FROM numbers(100) SETTINGS limit = 1) UNION ALL (SELECT number FROM numbers(100) SETTINGS limit = 2));

SELECT '-- a trailing query-level SETTINGS limit still caps the whole union (3, not 5 + 3)';
SELECT count() FROM (SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) SETTINGS limit = 3);

SELECT '-- construction settings are result modifiers, so they are IGNORED for INSERT ... SELECT (count = 10, not 2)';
DROP TABLE IF EXISTS t_construction_settings;
CREATE TABLE t_construction_settings (x UInt64) ENGINE = Memory;
INSERT INTO t_construction_settings SETTINGS limit = 2 SELECT number FROM numbers(10);
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

SELECT '-- construction settings are likewise IGNORED for CREATE ... AS SELECT (count = 10, not 3)';
DROP TABLE IF EXISTS t_create_as_select;
CREATE TABLE t_create_as_select ENGINE = Memory AS SELECT number FROM numbers(10) SETTINGS limit = 3;
SELECT count() FROM t_create_as_select;
DROP TABLE t_create_as_select;
