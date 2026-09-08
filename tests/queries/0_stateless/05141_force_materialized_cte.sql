-- `force_materialized_cte` (default 1) throws when a CTE declared `AS MATERIALIZED` would be silently inlined.

SET enable_analyzer = 1;
SET enable_materialized_cte = 0;
-- With `force_materialized_cte = 0` the analyzer warns that `MATERIALIZED` is ignored; keep it out of stderr.
SET send_logs_level = 'fatal';

SELECT 'analyzer, materialization disabled: throws';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
-- An unreferenced materialized CTE throws too.
WITH c AS MATERIALIZED (SELECT 1) SELECT 2; -- { serverError SUPPORT_IS_DISABLED }
-- Nested in a subquery.
SELECT * FROM (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b); -- { serverError SUPPORT_IS_DISABLED }
-- A view with such a definition cannot be read either. The view is created while materialization is enabled
-- (a `SETTINGS` clause inside the view's `SELECT` would be stored in the definition and re-applied at read time).
SET enable_materialized_cte = 1;
CREATE VIEW v_force_materialized_cte AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b;
SET enable_materialized_cte = 0;
SELECT * FROM v_force_materialized_cte; -- { serverError SUPPORT_IS_DISABLED }
DROP VIEW v_force_materialized_cte;

SELECT 'analyzer, force disabled: inlined';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS force_materialized_cte = 0;
WITH c AS MATERIALIZED (SELECT 1) SELECT 2 SETTINGS force_materialized_cte = 0;

SELECT 'analyzer, materialization enabled: works';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1;
-- Enabling it for a subquery only is enough for the CTEs of that subquery.
SELECT * FROM (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1);

SELECT 'compatibility restores the old behaviour';
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS compatibility = '26.8';

SELECT 'old analyzer: throws, force disabled inlines';
SET enable_analyzer = 0;
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1; -- { serverError SUPPORT_IS_DISABLED }
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b SETTINGS force_materialized_cte = 0;
