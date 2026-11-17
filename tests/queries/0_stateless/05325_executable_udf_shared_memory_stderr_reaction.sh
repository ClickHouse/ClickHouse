#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# How `stderr_reaction` applies to a single query of the shared-memory transport: stderr a command
# writes at startup, just before or on its way out, or after answering. Each scenario runs in a
# `clickhouse-local` of its own, so its log is its own. What a pooled worker leaves behind between
# borrows is in `05324_executable_udf_shared_memory_stderr`; timeouts are covered by
# `05565_executable_udf_shared_memory_timeouts`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    shm_function shm_stderr_at_startup executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>throw</stderr_reaction><shared_memory_size>4096</shared_memory_size>" "shm_udf.py --stderr-at-startup"
    shm_function shm_stderr_then_exit executable "<stderr_reaction>throw</stderr_reaction><shared_memory_size>16777216</shared_memory_size>" "shm_udf_noisy.py --after-answer stderr-then-exit"
    shm_function shm_on_the_way_out executable "<stderr_reaction>throw</stderr_reaction><shared_memory_size>4096</shared_memory_size>" "shm_udf_noisy.py --after-answer stderr-on-the-way-out"
    shm_function shm_on_the_way_out_no_exit_check executable \
        "<stderr_reaction>throw</stderr_reaction><check_exit_code>0</check_exit_code><shared_memory_size>4096</shared_memory_size>" "shm_udf_noisy.py --after-answer stderr-on-the-way-out"
    shm_function shm_stderr_after_stdout executable "<stderr_reaction>none</stderr_reaction><shared_memory_size>4096</shared_memory_size>" "shm_udf_noisy.py --after-answer stderr-flood-on-the-way-out"
    shm_function shm_chatty_stderr_throw executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>throw</stderr_reaction><shared_memory_size>16777216</shared_memory_size>" "shm_udf_noisy.py --report-pid --stderr-before-frame line"
    shm_function shm_chatty_stderr_no_exit_check executable_pool \
        "<pool_size>1</pool_size><check_exit_code>0</check_exit_code><stderr_reaction>throw</stderr_reaction><shared_memory_size>16777216</shared_memory_size>" \
        "shm_udf_noisy.py --report-pid --stderr-before-frame line"
} | shm_functions

# The output of a query whose answer is a single row of half a million: parsing those out of the
# region takes long enough that the command has provably done what it does after answering.
BIG="FROM numbers(500000) SETTINGS max_threads = 1, max_block_size = 500000"

echo "--- stderr of a fresh pooled worker at startup fails the query"
# The process is new for this borrow, so what it writes before its first request is this query's.
shm_local "
    SELECT shm_stderr_at_startup(1);
"
shm_log_contains "starting up"

echo "--- stderr written just before exit still throws"
shm_local "
    SELECT DISTINCT shm_stderr_then_exit(number) $BIG FORMAT Null;
"
shm_log_contains "eeee"

echo "--- stderr written on the way out still throws, with or without the exit code checked"
# Found by the bounded wait that reaps the command, the last stretch in which it can write at all.
# The reaction does not depend on \`check_exit_code\`, an unrelated setting.
shm_local "
    SELECT shm_on_the_way_out(1) FORMAT Null;
"
shm_log_contains "complaining on the way out"
shm_local "
    SELECT shm_on_the_way_out_no_exit_check(1) FORMAT Null;
"
shm_log_contains "complaining on the way out"

echo "--- stderr written after closing stdout does not hang the reap"
shm_local "
    SELECT shm_stderr_after_stdout(1);
"

echo "--- a line on stderr with the answer under throw fails the query that caused it"
# Written before the response frame, so the query is still waiting for the command when it comes.
# The worker is then discarded, so every query fails for its own line and none for a previous one -
# also with \`check_exit_code\` off, where nothing about the worker says what was decided about it.
shm_local "
    SELECT shm_chatty_stderr_throw(1) FORMAT Null;
    SELECT shm_chatty_stderr_throw(1) FORMAT Null;
    SELECT shm_chatty_stderr_no_exit_check(1) FORMAT Null;
    SELECT shm_chatty_stderr_no_exit_check(1) FORMAT Null;
    SELECT shm_chatty_stderr_no_exit_check(1) FORMAT Null;
"
grep -cF "Executable generates stderr: done" "$SHM_UDF_WORK/local.log" | awk '{ print ($1 >= 5) }'
