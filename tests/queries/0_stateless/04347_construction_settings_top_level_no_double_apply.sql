-- Tags: no-old-analyzer
-- Regression: query-construction settings on the OUTERMOST query must be materialized exactly once.
-- `applyQueryConstructionSettings` re-attaches the query-level `SETTINGS` clause to the outer wrapped
-- query; the consumed construction settings (`select` / `filter` / `order` / `sort`) must be stripped
-- from it, otherwise the nested-settings pass wraps the query a second time -- e.g. `select = 'number
-- AS x'` exposes only `x` in the first wrap and then fails to resolve `number` in the second.
-- The old analyzer renders the wrapping differently, so this runs only on the (default) new analyzer.

SELECT '-- select on the outermost query (SETTINGS on the inner SELECT)';
SELECT number FROM numbers(3) SETTINGS select = 'number AS x', order = 'x';

SELECT '-- select on the outermost query (FORMAT-suffix SETTINGS, stored on the union node)';
SELECT number FROM numbers(3) FORMAT TSV SETTINGS select = 'number AS x', order = 'x';

SELECT '-- filter on the outermost query is applied exactly once';
SELECT number FROM numbers(10) SETTINGS filter = 'number > 7', order = 'number';

SELECT '-- order on the outermost query is applied exactly once';
SELECT number FROM numbers(3) SETTINGS order = '-number';

SELECT '-- select + filter + order together on the outermost query';
SELECT number FROM numbers(10) SETTINGS select = 'number AS x', filter = 'number > 7', order = 'x';
