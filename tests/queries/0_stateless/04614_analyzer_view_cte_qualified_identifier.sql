-- Views expand CTE references in FROM into subqueries with cte_name set
-- (ApplyWithSubqueryVisitor). The analyzer must propagate cte_name so
-- qualified identifiers like `c.x` still bind.
-- https://github.com/ClickHouse/clickhouse-private/issues/55715#issuecomment-5004050349

DROP TABLE IF EXISTS v_cte_old, v_cte_new, v_cte_both, v_cte_noalias, v_cte_union, v_cte_double, mv_cte, t_mv_src;

SELECT '-- view created with enable_analyzer = 0, aliased CTE reference';
SET enable_analyzer = 0;
CREATE VIEW v_cte_old AS WITH c AS (SELECT 1 AS x) SELECT c.x AS f FROM c AS s;
SELECT * FROM v_cte_old SETTINGS enable_analyzer = 0;
SELECT * FROM v_cte_old SETTINGS enable_analyzer = 1;

SELECT '-- view created with enable_analyzer = 1';
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

SELECT '-- two references to the same CTE with different aliases';
CREATE VIEW v_cte_double AS WITH c AS (SELECT 1 AS x) SELECT c.x AS f FROM c AS s1, c AS s2;
SELECT * FROM v_cte_double SETTINGS enable_analyzer = 0; -- { serverError AMBIGUOUS_COLUMN_NAME }
SELECT * FROM v_cte_double SETTINGS enable_analyzer = 1;

SELECT '-- materialized view with an aliased CTE reference';
CREATE TABLE t_mv_src (a UInt8) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_cte ENGINE = MergeTree ORDER BY f AS WITH c AS (SELECT a AS x FROM t_mv_src) SELECT c.x AS f FROM c AS s;
SET enable_analyzer = 0;
INSERT INTO t_mv_src VALUES (1);
SET enable_analyzer = 1;
INSERT INTO t_mv_src VALUES (2);
SELECT * FROM mv_cte ORDER BY f;

DROP TABLE v_cte_old, v_cte_new, v_cte_both, v_cte_noalias, v_cte_union, v_cte_double, mv_cte, t_mv_src;
