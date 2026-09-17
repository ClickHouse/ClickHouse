-- Tags: no-parallel
-- `allow_executable_tables` gates the `executable` table function and the
-- `Executable` and `ExecutablePool` table engines.

DROP TABLE IF EXISTS t_exec_gate;

SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError UNSUPPORTED_METHOD }

SET allow_executable_tables = 0;
SELECT * FROM executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_gate (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_gate (x UInt32) ENGINE = ExecutablePool('nonexist.sh', 'TSV'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_as2 (x UInt32) AS executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_nested (x UInt32) AS loop(executable('nonexist.sh', 'TSV', 'x UInt32')); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_nested (x UInt32) ENGINE = Remote('127.0.0.1', executable('nonexist.sh', 'TSV', 'x UInt32')); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_exec_nested AS remote('127.0.0.1:65535', executable('nonexist.sh', 'TSV', 'x UInt32')); -- { serverError SUPPORT_IS_DISABLED }

-- A full-definition ATTACH is fresh user input, not replay, so the gate applies.
ATTACH TABLE t_exec_gate UUID '00000000-0000-0000-0000-000000005218' (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV'); -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE t_exec_as2 UUID '00000000-0000-0000-0000-000000005219' (`x` UInt32) AS executable('nonexist.sh', 'TSV', 'x UInt32'); -- { serverError SUPPORT_IS_DISABLED }

-- A table created while allowed still attaches once disabled, so a server already holding one
-- starts up. It keeps its metadata and can be dropped or detached, but cannot be read.
SET allow_executable_tables = 1;
CREATE TABLE t_exec_gate (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV');
SET allow_executable_tables = 0;
DETACH TABLE t_exec_gate;
ATTACH TABLE t_exec_gate;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_gate';
SELECT * FROM t_exec_gate; -- { serverError SUPPORT_IS_DISABLED }
SET allow_executable_tables = 1;
SELECT * FROM t_exec_gate; -- { serverError UNSUPPORTED_METHOD }
SET allow_executable_tables = 0;
DROP TABLE t_exec_gate;

-- Stored `CREATE TABLE ... AS executable(...)` metadata is replayed by calling the table function
-- again, so constructing the storage must stay allowed or the table could never be reattached.
SET allow_executable_tables = 1;
CREATE TABLE t_exec_as (x UInt32) AS executable('nonexist.sh', 'TSV', 'x UInt32');
DETACH TABLE t_exec_as;
SET allow_executable_tables = 0;
ATTACH TABLE t_exec_as;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_as';
SELECT * FROM t_exec_as; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE t_exec_as;
