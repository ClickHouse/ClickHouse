-- The granules skipped by the query condition cache of the `Memory` engine are removed before
-- PREWHERE and the row policy, so the cache must not be used when they depend on the set of rows.

SET use_query_condition_cache = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS t_memory_qcc_stateful;
CREATE TABLE t_memory_qcc_stateful (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_qcc_stateful SELECT number FROM numbers(16384) SETTINGS max_block_size = 20000;

-- Records that the first granule has no rows with `k = 8192`.
SELECT count() FROM t_memory_qcc_stateful WHERE k = 8192 SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_memory_qcc_stateful WHERE k = 8192 SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- PREWHERE';
SELECT groupArray(k) FROM t_memory_qcc_stateful PREWHERE rowNumberInBlock() = 0 WHERE k = 8192;
SELECT groupArray(k) FROM t_memory_qcc_stateful PREWHERE blockSize() = 8192 WHERE k = 8192;
SELECT groupArray(k) FROM t_memory_qcc_stateful PREWHERE rowNumberInBlock() = 0 AND k >= 0 WHERE k = 8192;

SELECT '-- row policy';
DROP ROW POLICY IF EXISTS p_memory_qcc_stateful ON t_memory_qcc_stateful;
CREATE ROW POLICY p_memory_qcc_stateful ON t_memory_qcc_stateful USING rowNumberInBlock() != 0 TO ALL;
SELECT groupArray(k) FROM t_memory_qcc_stateful WHERE k = 8192 SETTINGS optimize_move_to_prewhere = 0;
SELECT groupArray(k) FROM t_memory_qcc_stateful PREWHERE k = 8192;
DROP ROW POLICY p_memory_qcc_stateful ON t_memory_qcc_stateful;

DROP TABLE t_memory_qcc_stateful;
