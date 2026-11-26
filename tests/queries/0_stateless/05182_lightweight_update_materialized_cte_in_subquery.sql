-- Lightweight UPDATE / DELETE whose IN subquery defines a materialized CTE.
-- The mutation plan must gate the CTE readers like the Planner does for a SELECT,
-- also when the set is built at run time (empty table, non-key column, index analysis for IN disabled).

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DROP TABLE IF EXISTS t_lwu_materialized_cte;

CREATE TABLE t_lwu_materialized_cte (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- Empty table: nothing to read, the set is built at run time.
UPDATE t_lwu_materialized_cte SET v = 1 WHERE id IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT count() FROM t_lwu_materialized_cte;

INSERT INTO t_lwu_materialized_cte SELECT number, number FROM numbers(5);

-- SELECT analogue: the Planner plants the gate itself.
SELECT count() FROM t_lwu_materialized_cte WHERE v IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);

-- Non-key column: no primary-key analysis, the set is built at run time.
UPDATE t_lwu_materialized_cte SET v = 100 WHERE v IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT * FROM t_lwu_materialized_cte ORDER BY id;

-- Key column with index analysis for IN disabled: the set is built at run time.
SET use_index_for_in_with_subqueries = 0;
UPDATE t_lwu_materialized_cte SET v = 200 WHERE id IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT * FROM t_lwu_materialized_cte ORDER BY id;

-- Two IN subqueries, each defining its own materialized CTE named c: one gate per set subquery.
UPDATE t_lwu_materialized_cte SET v = 250 WHERE v IN (WITH c AS MATERIALIZED (SELECT number + 200 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b) AND id IN (WITH c AS MATERIALIZED (SELECT number * 2 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT * FROM t_lwu_materialized_cte ORDER BY id;

-- A materialized CTE depending on another one inside the set subquery: two dependency levels.
UPDATE t_lwu_materialized_cte SET v = 260 WHERE v IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)), d AS MATERIALIZED (SELECT x + 200 AS y FROM c) SELECT a.y FROM d AS a, d AS b);
SELECT * FROM t_lwu_materialized_cte ORDER BY id;

-- Key column with index analysis: the set is built in place during primary-key analysis.
SET use_index_for_in_with_subqueries = 1;
UPDATE t_lwu_materialized_cte SET v = 300 WHERE id IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT * FROM t_lwu_materialized_cte ORDER BY id;

-- Lightweight DELETE goes through the same mutation plan.
DELETE FROM t_lwu_materialized_cte WHERE v IN (WITH c AS MATERIALIZED (SELECT number + 300 AS x FROM numbers(3)) SELECT a.x FROM c AS a, c AS b);
SELECT * FROM t_lwu_materialized_cte ORDER BY id;

DROP TABLE t_lwu_materialized_cte;
