-- Test coverage for ExecutableSettings::loadFromQuery() and related methods.
-- CREATE TABLE with a SETTINGS clause calls loadFromQuery(), which was previously uncovered.
-- No actual script execution is needed: the file existence check is deferred to SELECT time.

DROP TABLE IF EXISTS t_exec_settings;

-- All builtin settings explicitly specified — covers loadFromQuery() + impl->applyChanges()
CREATE TABLE t_exec_settings (x UInt32)
ENGINE = Executable('nonexist.sh', 'TSV')
SETTINGS
    send_chunk_header = 1,
    pool_size = 4,
    max_command_execution_time = 30,
    command_termination_timeout = 5,
    command_read_timeout = 5000,
    command_write_timeout = 5000,
    check_exit_code = 1;

SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_settings';

DROP TABLE t_exec_settings;

-- ExecutablePool with SETTINGS
DROP TABLE IF EXISTS t_exec_pool_settings;

CREATE TABLE t_exec_pool_settings (x UInt32)
ENGINE = ExecutablePool('nonexist.sh', 'TSV')
SETTINGS
    send_chunk_header = 0,
    pool_size = 2,
    check_exit_code = 0;

SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_exec_pool_settings';

DROP TABLE t_exec_pool_settings;

-- Unknown setting should be rejected
CREATE TABLE t_exec_bad_setting (x UInt32)
ENGINE = Executable('nonexist.sh', 'TSV')
SETTINGS nonexistent_setting = 1; -- {serverError UNKNOWN_SETTING}
