#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: the commands are waited for through `/proc`

# `stderr_reaction`, `check_exit_code` and the `command_termination_timeout` budget for a command that
# has finished writing but not exited, on the dictionary side: the same settings the executable user
# defined functions have. Executable dictionaries can be created with DDL only in `clickhouse-local`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

echo -n | shm_functions

# A dictionary named `name` over the command, with the source options given.
function source_dictionary()
{
    echo "CREATE DICTIONARY $1 (input UInt64, result String) PRIMARY KEY input
        SOURCE(EXECUTABLE(COMMAND '$2' FORMAT 'TabSeparated' EXECUTE_DIRECT 1 $3)) LAYOUT(FLAT()) LIFETIME(0);"
}

function pool_dictionary()
{
    echo "CREATE DICTIONARY $1 (input UInt64, result String) PRIMARY KEY input
        SOURCE(EXECUTABLE_POOL(COMMAND '$2' FORMAT 'TabSeparated' EXECUTE_DIRECT 1 POOL_SIZE 1 $3)) LAYOUT(DIRECT());"
}

echo "--- stderr of a source"
# The same source, which answers and complains on `stderr`, under the two ends of `stderr_reaction`.
# Under `throw` the diagnostic fails the load, and it is quoted in the exception so that the
# dictionary's `last_exception` says what the command said. Under `none` it is read off the pipe and
# dropped, and the load sees only the rows.
shm_local "
    $(source_dictionary stderr_throw dict_source_stderr.py "STDERR_REACTION 'throw'")
    $(source_dictionary stderr_none dict_source_stderr.py "STDERR_REACTION 'none'")
    SELECT * FROM dictionary(stderr_throw) ORDER BY input;
    SELECT last_exception LIKE '%Executable generates stderr%the source complains%' FROM system.dictionaries WHERE name = 'stderr_throw';
    SELECT * FROM dictionary(stderr_none) ORDER BY input;
"

echo "--- the exit code of a source"
# A source that produces every row and then exits with `3`. `check_exit_code` is on by default, so
# that is a failed load however complete the rows were; turned off, the rows are all that counts.
shm_local "
    $(source_dictionary exit_checked dict_source_exit_nonzero.py)
    $(source_dictionary exit_ignored dict_source_exit_nonzero.py "CHECK_EXIT_CODE 0")
    SELECT * FROM dictionary(exit_checked) ORDER BY input;
    SELECT status, last_exception LIKE '%Child process was exited with return code 3%' FROM system.dictionaries WHERE name = 'exit_checked';
    SELECT * FROM dictionary(exit_ignored) ORDER BY input;
"

echo "--- a source that lingers after its output"
# A source that closes its stdout after the rows and exits successfully only after its
# `command_termination_timeout` (one second here). With `check_exit_code` on, its exit status is
# waited for without a bound, as it always was, and the load succeeds. With it off, the budget is
# spent, the command is signalled, and the rows are the result just the same.
shm_local "
    $(source_dictionary lingers dict_source_lingers.py "COMMAND_TERMINATION_TIMEOUT 1")
    $(source_dictionary lingers_unchecked dict_source_lingers.py "COMMAND_TERMINATION_TIMEOUT 1 CHECK_EXIT_CODE 0")
    SELECT * FROM dictionary(lingers) ORDER BY input;
    SELECT * FROM dictionary(lingers_unchecked) ORDER BY input;
"

echo "--- stderr of a pooled source"
# A worker that answers a key and complains on `stderr` at the same time. Under `throw` the request
# that caused the line fails; under `none` the line is dropped and the answer is what comes back.
shm_local "
    $(pool_dictionary pool_stderr_throw dict_input_stderr.py "STDERR_REACTION 'throw'")
    $(pool_dictionary pool_stderr_none dict_input_stderr.py "STDERR_REACTION 'none'")
    SELECT dictGet('pool_stderr_throw', 'result', toUInt64(1));
    SELECT dictGet('pool_stderr_none', 'result', toUInt64(1));
"
shm_output_contains "the command complains"

echo "--- a pooled source that lingers after its output"
# The worker answers the key, closes its stdout and stays alive. A worker without a stdout cannot
# answer anyone else, so it is discarded, and it has `command_termination_timeout` (one second here)
# to exit. With `check_exit_code` on, an exit code that could not be read within that budget fails
# the request; with it off, the budget is spent, the worker is signalled, and the answer it gave is
# the result.
shm_local "
    $(pool_dictionary pool_lingers dict_input_lingers.py "COMMAND_TERMINATION_TIMEOUT 1")
    $(pool_dictionary pool_lingers_unchecked dict_input_lingers.py "COMMAND_TERMINATION_TIMEOUT 1 CHECK_EXIT_CODE 0")
    SELECT dictGet('pool_lingers', 'result', toUInt64(1));
    SELECT dictGet('pool_lingers_unchecked', 'result', toUInt64(1));
"
shm_output_contains "did not exit within command_termination_timeout (1 seconds)"
