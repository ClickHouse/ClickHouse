-- The old interpreter has no CTE name resolution of its own, so on the query paths that still reach
-- it a `MATERIALIZED` CTE is inlined at every reference, as a plain one.
-- https://github.com/ClickHouse/ClickHouse/issues/113711

SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_old_113711b;
CREATE TABLE t_old_113711b (x UInt8) ENGINE = MergeTree ORDER BY x;

SELECT '-- EXPLAIN SYNTAX of a non-SELECT statement';
EXPLAIN SYNTAX INSERT INTO t_old_113711b WITH c_old_113711b AS MATERIALIZED (SELECT 1 AS x) SELECT x FROM c_old_113711b;

SELECT '-- EXPLAIN AST with optimize = 1';
EXPLAIN AST optimize = 1 WITH c_old_113711b AS MATERIALIZED (SELECT 1 AS x) SELECT x FROM c_old_113711b;

DROP TABLE t_old_113711b;
