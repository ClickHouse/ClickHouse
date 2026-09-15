-- Test that `allow_executable_table_function` and `allow_executable_table_engine` gate their own
-- surface and nothing else. No script execution is needed: the file existence check is deferred to
-- SELECT time, so a missing script is the "allowed" outcome.

DROP TABLE IF EXISTS t_exec_gate;

SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError UNSUPPORTED_METHOD }

-- Table function off: the function and DESCRIBE are refused, the engine is untouched.
SET allow_executable_table_function = 0;
SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError SUPPORT_IS_DISABLED }
DESCRIBE executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_gate (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV');
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_gate';
DROP TABLE t_exec_gate;

-- Table engine off: both engines are refused, the table function is untouched.
SET allow_executable_table_function = 1;
SET allow_executable_table_engine = 0;
CREATE TABLE t_exec_gate (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_gate (x UInt32) ENGINE = ExecutablePool('nonexist.sh', 'TSV'); -- { serverError SUPPORT_IS_DISABLED }
SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError UNSUPPORTED_METHOD }

-- A table created while allowed keeps its metadata, but cannot be read once the engine is refused.
SET allow_executable_table_engine = 1;
CREATE TABLE t_exec_gate (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV');
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_gate';
SET allow_executable_table_engine = 0;
SELECT * FROM t_exec_gate; -- { serverError SUPPORT_IS_DISABLED }
SET allow_executable_table_engine = 1;
SELECT * FROM t_exec_gate; -- { serverError UNSUPPORTED_METHOD }
DROP TABLE t_exec_gate;

-- ATTACH is deliberately ungated, so a server already holding such a table still starts up.
SET allow_executable_table_engine = 1;
CREATE TABLE t_exec_attach (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV');
DETACH TABLE t_exec_attach;
SET allow_executable_table_engine = 0;
ATTACH TABLE t_exec_attach;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_attach';
SELECT * FROM t_exec_attach; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE t_exec_attach;
