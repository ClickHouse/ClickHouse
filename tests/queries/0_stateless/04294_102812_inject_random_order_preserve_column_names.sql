-- Regression test for issues #102812 and #101107.
-- With `inject_random_order_for_select_without_order_by = 1`, the analyzer
-- previously wrapped the query in `SELECT __subquery_column_UUID FROM (...) ORDER BY rand()`,
-- which: (a) corrupted output column names visible to CTAS / VIEW / named output formats,
-- and (b) crashed with `BAD_ARGUMENTS` for any multi-column SELECT because a single
-- alias was applied to N projection columns.
SET inject_random_order_for_select_without_order_by = 1;
SET enable_analyzer = 1;

-- Single-column CTAS: column name must be `number`, not the internal UUID alias.
DROP TABLE IF EXISTS t_ctas_single;
CREATE TABLE t_ctas_single ENGINE = Memory AS SELECT number FROM numbers(1);
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_ctas_single' ORDER BY name;
DROP TABLE t_ctas_single;

-- Multi-column CTAS: all three column names must be preserved and the query must not crash.
DROP TABLE IF EXISTS t_ctas_multi;
CREATE TABLE t_ctas_multi ENGINE = Memory AS SELECT 1 AS a, 2 AS b, 3 AS c;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_ctas_multi' ORDER BY name;
DROP TABLE t_ctas_multi;

-- CREATE VIEW: column name must come from the SELECT, not from the internal alias.
DROP VIEW IF EXISTS v_select_single;
CREATE VIEW v_select_single AS SELECT number FROM numbers(1);
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'v_select_single';
DROP VIEW v_select_single;

-- Multi-column SELECT without ORDER BY must not crash.
SELECT number, number + 1 FROM numbers(3) FORMAT Null;

-- UNION ALL with multi-column branches must not crash.
SELECT number, number + 1 AS n1 FROM numbers(2)
UNION ALL
SELECT number, number + 2 AS n1 FROM numbers(2)
FORMAT Null;

-- INSERT INTO ... SELECT: the column-name-matched path must not corrupt the destination schema.
DROP TABLE IF EXISTS t_insert_dst;
CREATE TABLE t_insert_dst (number UInt64) ENGINE = Memory;
INSERT INTO t_insert_dst SELECT number FROM numbers(2);
SELECT count() FROM t_insert_dst;
DROP TABLE t_insert_dst;

-- ORDER BY rand() injection must still happen for the wrapped query.
-- A simple SELECT should produce a Sorting step in the plan; a SELECT with explicit
-- ORDER BY must not produce a second one.
SELECT count() FROM (EXPLAIN PLAN SELECT number FROM numbers(5)) WHERE explain LIKE '%Sorting%';
SELECT count() FROM (EXPLAIN PLAN SELECT number FROM numbers(5) ORDER BY number) WHERE explain LIKE '%Sorting%';
