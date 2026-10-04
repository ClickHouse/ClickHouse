-- The in-source filter of a `Memory` table returns only the rows that passed, and nothing at all
-- for a block where no row passed, so the read progress has to be reported explicitly: the number
-- of scanned rows is what `max_rows_to_read`, the read quotas and `SelectedRows` are based on.

-- The test harness may randomize this setting, and the queries below rely on it.
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_memory_read_rows;

CREATE TABLE t_memory_read_rows (k UInt64, v String) ENGINE = Memory;

-- Several inserts, so the table consists of multiple blocks.
INSERT INTO t_memory_read_rows SELECT number, toString(number) FROM numbers(0, 500);
INSERT INTO t_memory_read_rows SELECT number, toString(number) FROM numbers(500, 500);

-- One row passes.
SELECT count() FROM t_memory_read_rows PREWHERE k = 500;
-- No row passes, and no block produces a chunk at all.
SELECT count() FROM t_memory_read_rows PREWHERE k >= 1000;
-- No filter, for comparison.
SELECT sum(k) FROM t_memory_read_rows;

SYSTEM FLUSH LOGS query_log;

-- All three read all the 1000 rows of the table.
SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%FROM t_memory_read_rows%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_memory_read_rows;
