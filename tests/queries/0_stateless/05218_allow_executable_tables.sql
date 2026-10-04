-- `allow_executable_tables` gates reading through the `executable` table function and from
-- `Executable` and `ExecutablePool` tables.

SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError UNSUPPORTED_METHOD }

SET allow_executable_tables = 0;
SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError SUPPORT_IS_DISABLED }
DESCRIBE executable('nonexist.sh', 'TSV', 'x UInt32');

-- A definition is still accepted and can be managed; only reading it is refused.
CREATE TABLE t_exec_gate (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV');
CREATE VIEW v_exec_gate AS SELECT * FROM t_exec_gate;
CREATE MATERIALIZED VIEW mv_exec_gate ENGINE = MergeTree ORDER BY x AS SELECT * FROM t_exec_gate;
CREATE TABLE t_exec_pool_gate (x UInt32) ENGINE = ExecutablePool('nonexist.sh', 'TSV');
DETACH TABLE t_exec_gate;
ATTACH TABLE t_exec_gate;

SELECT * FROM t_exec_gate; -- { serverError SUPPORT_IS_DISABLED }
SELECT * FROM v_exec_gate; -- { serverError SUPPORT_IS_DISABLED }
SELECT * FROM t_exec_pool_gate; -- { serverError SUPPORT_IS_DISABLED }

-- The gate is evaluated on the node that runs the read, so a distributed wrapper does not lift it.
SELECT * FROM remote('127.0.0.1', executable('nonexist.sh', 'TSV', 'x UInt32')); -- { serverError SUPPORT_IS_DISABLED }

-- A statement that reads as part of its own execution is refused too.
CREATE TABLE t_exec_gate_copy ENGINE = MergeTree ORDER BY x AS SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError SUPPORT_IS_DISABLED }
CREATE MATERIALIZED VIEW mv_exec_gate_populate ENGINE = MergeTree ORDER BY x POPULATE AS SELECT * FROM t_exec_gate; -- { serverError SUPPORT_IS_DISABLED }

SET allow_executable_tables = 1;
SELECT * FROM t_exec_gate; -- { serverError UNSUPPORTED_METHOD }

SET allow_executable_tables = 0;
DROP VIEW mv_exec_gate;
DROP VIEW v_exec_gate;
DROP TABLE t_exec_gate;
