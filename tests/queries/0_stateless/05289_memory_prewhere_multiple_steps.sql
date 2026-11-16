-- A conjunction in PREWHERE over a `Memory` table is split into steps (`enable_multiple_prewhere_read_steps`),
-- and each step reads its columns only for the rows that passed the previous steps.
-- The results must not depend on the split.

DROP TABLE IF EXISTS t_memory_prewhere_steps;

CREATE TABLE t_memory_prewhere_steps (k UInt64, s String, tup Tuple(a UInt64, b String))
ENGINE = Memory SETTINGS compress = true;

-- Several inserts, so the table consists of multiple blocks.
INSERT INTO t_memory_prewhere_steps SELECT number, repeat('x', 100), (number, toString(number)) FROM numbers(0, 1000);
INSERT INTO t_memory_prewhere_steps SELECT number, repeat('y', 100), (number, toString(number)) FROM numbers(1000, 1000);

SELECT 'basic';
SELECT count(), sum(length(s)) FROM t_memory_prewhere_steps PREWHERE k = 5 AND s != '' SETTINGS enable_multiple_prewhere_read_steps = 1;
SELECT count(), sum(length(s)) FROM t_memory_prewhere_steps PREWHERE k = 5 AND s != '' SETTINGS enable_multiple_prewhere_read_steps = 0;

SELECT 'output columns in a different order';
SELECT s, k FROM t_memory_prewhere_steps PREWHERE k IN (5, 1005) AND s != '' ORDER BY k SETTINGS enable_multiple_prewhere_read_steps = 1;

SELECT 'a condition that may throw is evaluated only for the rows that passed the previous ones';
SELECT count() FROM t_memory_prewhere_steps PREWHERE k % 1000 != 0 AND intDiv(10, k % 1000) >= 0 SETTINGS enable_multiple_prewhere_read_steps = 1;
SELECT count() FROM t_memory_prewhere_steps PREWHERE k % 1000 != 0 AND intDiv(10, k % 1000) >= 0 SETTINGS enable_multiple_prewhere_read_steps = 0;

SELECT 'kept PREWHERE column';
SELECT k, (k < 3 AND s != '') AS c FROM t_memory_prewhere_steps PREWHERE c ORDER BY k SETTINGS enable_multiple_prewhere_read_steps = 1;

SELECT 'subcolumns';
SELECT k, tup.b FROM t_memory_prewhere_steps PREWHERE tup.a >= 1998 AND tup.b != '' AND s != '' ORDER BY k SETTINGS enable_multiple_prewhere_read_steps = 1;

SELECT 'moved from WHERE';
SELECT k, s FROM t_memory_prewhere_steps WHERE s != '' AND k = 1500 SETTINGS enable_multiple_prewhere_read_steps = 1, optimize_move_to_prewhere = 1;

SELECT 'a stateful function sees all rows, not only those that passed the other conditions';
DROP TABLE IF EXISTS t_memory_prewhere_steps_stateful;
CREATE TABLE t_memory_prewhere_steps_stateful (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_prewhere_steps_stateful VALUES (0), (1), (2), (3), (4), (5), (6), (7);
SELECT groupArray(k) FROM t_memory_prewhere_steps_stateful PREWHERE k % 2 = 1 AND rowNumberInBlock() < 4 SETTINGS enable_multiple_prewhere_read_steps = 1;
SELECT groupArray(k) FROM t_memory_prewhere_steps_stateful PREWHERE k % 2 = 1 AND rowNumberInBlock() < 4 SETTINGS enable_multiple_prewhere_read_steps = 0;

SELECT 'a function non-deterministic in scope of the query sees all rows, not only those that passed the other conditions';
SELECT groupArray(k) FROM t_memory_prewhere_steps_stateful PREWHERE k % 2 = 1 AND blockSize() = 8 SETTINGS enable_multiple_prewhere_read_steps = 1;
SELECT groupArray(k) FROM t_memory_prewhere_steps_stateful PREWHERE k % 2 = 1 AND blockSize() = 8 SETTINGS enable_multiple_prewhere_read_steps = 0;
DROP TABLE t_memory_prewhere_steps_stateful;

SELECT 'column added after the data was inserted';
ALTER TABLE t_memory_prewhere_steps ADD COLUMN n UInt64;
SELECT k, n FROM t_memory_prewhere_steps PREWHERE k = 5 AND n = 0 SETTINGS enable_multiple_prewhere_read_steps = 1;
SELECT k, n FROM t_memory_prewhere_steps PREWHERE n = 0 AND k = 5 SETTINGS enable_multiple_prewhere_read_steps = 1;

-- The column of the second condition is read only for the block where the first one has passing rows.
-- The query condition cache would skip the other block in both queries.
SELECT count() FROM t_memory_prewhere_steps PREWHERE k = 5 AND s != '' SETTINGS enable_multiple_prewhere_read_steps = 1, log_comment = '05289_multiple_steps', use_query_condition_cache = 0;
SELECT count() FROM t_memory_prewhere_steps PREWHERE k = 5 AND s != '' SETTINGS enable_multiple_prewhere_read_steps = 0, log_comment = '05289_single_step', use_query_condition_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT 'fewer bytes read with multiple steps';
SELECT
    (SELECT read_bytes FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05289_multiple_steps')
    < (SELECT read_bytes FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05289_single_step');

DROP TABLE t_memory_prewhere_steps;
