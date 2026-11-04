-- The values of a `PREWHERE` filter column other than 0 and 1 mean that the row passes.
-- The `Memory` source must not record the granules of such rows as having no matches in the query condition cache.

SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS t_memory_qcc_nonbinary;
CREATE TABLE t_memory_qcc_nonbinary (k UInt8, s String) ENGINE = Memory;
INSERT INTO t_memory_qcc_nonbinary VALUES (0, 'a'), (2, 'b'), (3, 'c'), (3, 'd');

SELECT k, s FROM t_memory_qcc_nonbinary PREWHERE k ORDER BY s;
SELECT k, s FROM t_memory_qcc_nonbinary PREWHERE k ORDER BY s;
SELECT sum(k) FROM t_memory_qcc_nonbinary WHERE k SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

DROP TABLE t_memory_qcc_nonbinary;
