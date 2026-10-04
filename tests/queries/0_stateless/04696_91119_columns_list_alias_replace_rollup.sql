-- Tags: no-old-analyzer
-- no-old-analyzer: resolving `COLUMNS(alias)` to the aliased column requires the analyzer.

-- An unqualified `COLUMNS(identifier)` matcher produces the name of the column that the
-- identifier resolves to, not the identifier itself: `COLUMNS(alias)` over `c AS alias` produces
-- `c`. The deferred `group_by_use_nulls` REPLACE rewrite must therefore resolve the explicit
-- identifiers the same way the non-deferred path does, otherwise `group_by_use_nulls = 0` and
-- `group_by_use_nulls = 1` disagree on whether `c` is a replacement target.

SET enable_positional_arguments = 0;
SET prefer_column_name_to_alias = 0;

-- Both modes must reject this identically. Before the fix `group_by_use_nulls = 1` did not learn
-- that `COLUMNS(alias)` produces `c`, skipped the `c -> 100` replacement, and silently returned
-- rows instead of raising `NOT_AN_AGGREGATE`.
SELECT c AS alias, COLUMNS(alias) REPLACE (100 AS c)
FROM (SELECT 0 AS c)
GROUP BY c WITH ROLLUP
HAVING c > 50
SETTINGS group_by_use_nulls = 0; -- { serverError NOT_AN_AGGREGATE }

SELECT c AS alias, COLUMNS(alias) REPLACE (100 AS c)
FROM (SELECT 0 AS c)
GROUP BY c WITH ROLLUP
HAVING c > 50
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_AN_AGGREGATE }

-- Row-producing shape: the replacement value must survive into the super-aggregate row, and the
-- two modes must differ only by the `NULL` grouping markers.
SELECT c AS alias, COLUMNS(alias) REPLACE (100 AS c)
FROM (SELECT 0 AS c)
GROUP BY alias WITH ROLLUP
ORDER BY 1
SETTINGS group_by_use_nulls = 0;

SELECT c AS alias, COLUMNS(alias) REPLACE (100 AS c)
FROM (SELECT 0 AS c)
GROUP BY alias WITH ROLLUP
ORDER BY 1
SETTINGS group_by_use_nulls = 1;

-- Replacement expression referencing the replaced column itself.
SELECT c AS alias, COLUMNS(alias) REPLACE (c + 1 AS c)
FROM (SELECT 0 AS c)
GROUP BY c, alias WITH ROLLUP
ORDER BY 1, 2
SETTINGS group_by_use_nulls = 0;

SELECT c AS alias, COLUMNS(alias) REPLACE (c + 1 AS c)
FROM (SELECT 0 AS c)
GROUP BY c, alias WITH ROLLUP
ORDER BY 1, 2
SETTINGS group_by_use_nulls = 1;

-- An identifier naming the source column directly (the shape that was already covered) must be
-- unaffected by the alias-resolution path.
SELECT COLUMNS(c) REPLACE (100 AS c)
FROM (SELECT 0 AS c)
GROUP BY c WITH ROLLUP
HAVING c > 0
SETTINGS group_by_use_nulls = 0;

SELECT COLUMNS(c) REPLACE (100 AS c)
FROM (SELECT 0 AS c)
GROUP BY c WITH ROLLUP
HAVING c > 0
SETTINGS group_by_use_nulls = 1;
