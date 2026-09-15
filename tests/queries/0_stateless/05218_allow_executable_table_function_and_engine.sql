-- `allow_executable_table_function` gates the `executable` table function, and
-- `allow_executable_table_engine` gates the `Executable` and `ExecutablePool` table engines.
-- Each must gate only its own surface, so turning one off leaves the other working.

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

-- A table created while allowed still attaches once the engine is refused, so a server already
-- holding one starts up. It keeps its metadata and can be dropped or detached, but cannot be read.
SET allow_executable_table_engine = 1;
CREATE TABLE t_exec_gate (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV');
SET allow_executable_table_engine = 0;
DETACH TABLE t_exec_gate;
ATTACH TABLE t_exec_gate;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_gate';
SELECT * FROM t_exec_gate; -- { serverError SUPPORT_IS_DISABLED }
SET allow_executable_table_engine = 1;
SELECT * FROM t_exec_gate; -- { serverError UNSUPPORTED_METHOD }
SET allow_executable_table_engine = 0;
DROP TABLE t_exec_gate;
