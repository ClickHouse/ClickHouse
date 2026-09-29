-- A `PREWHERE` condition that is a column itself (`PREWHERE k`) keeps the values of the column
-- for the rows that pass. The `Memory` source must not replace them with a constant `1`.

DROP TABLE IF EXISTS t_memory_prewhere_column;

CREATE TABLE t_memory_prewhere_column (k UInt8, n Nullable(UInt8), f Float64, s String) ENGINE = Memory;
INSERT INTO t_memory_prewhere_column VALUES (0, NULL, 0, 'a'), (2, 5, 2.5, 'b'), (3, 0, -1, 'c'), (3, 7, 0, 'd');

SELECT 'explicit PREWHERE';
SELECT k, s FROM t_memory_prewhere_column PREWHERE k ORDER BY s;
SELECT n, s FROM t_memory_prewhere_column PREWHERE n ORDER BY s;
SELECT f, s FROM t_memory_prewhere_column PREWHERE f ORDER BY s;

SELECT 'moved WHERE';
SELECT k, s FROM t_memory_prewhere_column WHERE k ORDER BY s SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT n, s FROM t_memory_prewhere_column WHERE n ORDER BY s SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT 'GROUP BY and ORDER BY on the filter column';
SELECT k, count() FROM t_memory_prewhere_column WHERE k GROUP BY k ORDER BY k SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT k FROM t_memory_prewhere_column PREWHERE k ORDER BY k DESC;
SELECT sum(k) FROM t_memory_prewhere_column PREWHERE k;

DROP TABLE t_memory_prewhere_column;
