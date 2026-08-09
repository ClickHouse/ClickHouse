-- Tests that the eager readers of the system.parts family honor max_execution_time with
-- timeout_overflow_mode = 'break': the query stops early and returns the rows collected
-- so far (possibly none) instead of failing. The output goes to Null because the number
-- of returned rows depends on when the deadline fires.

DROP TABLE IF EXISTS t_system_parts_break;
CREATE TABLE t_system_parts_break (x UInt64, PROJECTION p (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_system_parts_break VALUES (1);
INSERT INTO t_system_parts_break VALUES (2);

-- Sanity check without any limits.
SELECT count() >= 2 FROM system.parts WHERE database = currentDatabase() AND table = 't_system_parts_break';

-- With a tiny time limit in the 'break' mode, the queries succeed instead of throwing TIMEOUT_EXCEEDED.
SELECT * FROM system.parts FORMAT Null SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';
SELECT * FROM system.parts_columns FORMAT Null SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';
SELECT * FROM system.projection_parts FORMAT Null SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';
SELECT * FROM system.projection_parts_columns FORMAT Null SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';
SELECT * FROM system.dropped_tables_parts FORMAT Null SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';

DROP TABLE t_system_parts_break;
