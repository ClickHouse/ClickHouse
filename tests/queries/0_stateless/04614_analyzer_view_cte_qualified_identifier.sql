-- Views expand CTE references in FROM into subqueries with cte_name set
-- (ApplyWithSubqueryVisitor). The analyzer must propagate cte_name so
-- qualified identifiers like `c.x` still bind.
-- https://github.com/ClickHouse/clickhouse-private/issues/55715#issuecomment-5004050349

DROP TABLE IF EXISTS v_cte_old, v_cte_new, v_cte_both, v_cte_noalias, v_cte_union;

SELECT '-- view created with old analyzer, aliased CTE reference';
SET enable_analyzer = 0;
CREATE VIEW v_cte_old AS WITH c AS (SELECT 1 AS x) SELECT c.x AS f FROM c AS s;
SELECT * FROM v_cte_old SETTINGS enable_analyzer = 0;
SELECT * FROM v_cte_old SETTINGS enable_analyzer = 1;

SELECT '-- view created with new analyzer';
SET enable_analyzer = 1;
CREATE VIEW v_cte_new AS WITH c AS (SELECT 1 AS x) SELECT c.x AS f FROM c AS s;
SELECT * FROM v_cte_new SETTINGS enable_analyzer = 0;
SELECT * FROM v_cte_new SETTINGS enable_analyzer = 1;

SELECT '-- qualification by both cte name and alias';
CREATE VIEW v_cte_both AS WITH c AS (SELECT 1 AS x) SELECT c.x AS cx, s.x AS sx FROM c AS s;
SELECT * FROM v_cte_both SETTINGS enable_analyzer = 0;
SELECT * FROM v_cte_both SETTINGS enable_analyzer = 1;

SELECT '-- unaliased CTE reference';
CREATE VIEW v_cte_noalias AS WITH c AS (SELECT 1 AS x) SELECT c.x AS f FROM c;
SELECT * FROM v_cte_noalias SETTINGS enable_analyzer = 0;
SELECT * FROM v_cte_noalias SETTINGS enable_analyzer = 1;

SELECT '-- UNION CTE body';
CREATE VIEW v_cte_union AS WITH c AS (SELECT 1 AS x UNION ALL SELECT 2 AS x) SELECT c.x AS f FROM c AS s;
SELECT * FROM v_cte_union ORDER BY f SETTINGS enable_analyzer = 0;
SELECT * FROM v_cte_union ORDER BY f SETTINGS enable_analyzer = 1;

SELECT '-- SHOW CREATE is unchanged';
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_cte_old';

DROP TABLE v_cte_old, v_cte_new, v_cte_both, v_cte_noalias, v_cte_union;
