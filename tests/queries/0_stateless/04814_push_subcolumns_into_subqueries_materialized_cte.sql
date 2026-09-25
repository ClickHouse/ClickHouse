-- Reused materialized CTEs stay TableNode references over the temporary table, so the subcolumn
-- pushdown deliberately does not apply to them (the temporary table serves all references of the
-- CTE, and pruning the parent column there would require proving that no reference needs the whole
-- column). The test checks that subcolumn reads over reused materialized CTEs stay correct with
-- the optimization enabled. Only JSON path subcolumns are used: other subcolumn reads over
-- materialized CTEs fail with NOT_FOUND_COLUMN_IN_BLOCK regardless of this optimization,
-- see https://github.com/ClickHouse/ClickHouse/issues/113623

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_push_subcolumns_mcte;

CREATE TABLE t_push_subcolumns_mcte (id UInt32, json JSON, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_mcte VALUES (1, '{"a": 1, "b": "x"}', (1, 'one')), (2, '{"a": 2, "b": "y"}', (2, 'two'));

SELECT 'reused materialized CTE';
WITH c AS MATERIALIZED (SELECT * FROM t_push_subcolumns_mcte)
SELECT l.json.a, r.json.b FROM c AS l INNER JOIN c AS r ON l.id = r.id ORDER BY l.id;

WITH c AS MATERIALIZED (SELECT * FROM t_push_subcolumns_mcte)
SELECT l.json.a, r.json.b FROM c AS l INNER JOIN c AS r ON l.id = r.id ORDER BY l.id
SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'whole column from reused materialized CTE';
WITH c AS MATERIALIZED (SELECT * FROM t_push_subcolumns_mcte)
SELECT l.tup, r.json FROM c AS l INNER JOIN c AS r ON l.id = r.id ORDER BY l.id;

DROP TABLE t_push_subcolumns_mcte;
