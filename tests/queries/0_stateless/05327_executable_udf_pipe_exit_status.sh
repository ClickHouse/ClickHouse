#!/usr/bin/env bash
# Tags: no-msan
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command

# How the pipe transport waits for a command once its output has ended: for its exit status under
# `check_exit_code`, for its last words on stderr, and how long - `command_termination_timeout` bounds
# what a command that will not leave may cost, without turning the wait for an exit status into a
# race. Each scenario runs in a `clickhouse-local` of its own. A query that sat out a termination
# timeout it should not have would fail for an exit status it did not get to see, so the answers
# alone tell the two apart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

function pipe_function()
{
    # name, type, the options that make it what it is, the command
    echo "<function><type>$2</type><name>$1</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format>$3<command>$4</command></function>"
}

{
    pipe_function pipe_on_the_way_out executable "<stderr_reaction>throw</stderr_reaction>" pipe_stderr_on_the_way_out.py
    pipe_function pipe_on_the_way_out_no_exit_check executable \
        "<stderr_reaction>throw</stderr_reaction><check_exit_code>0</check_exit_code>" pipe_stderr_on_the_way_out.py
    pipe_function pipe_lingers executable "<command_termination_timeout>2</command_termination_timeout>" pipe_lingers.py
    pipe_function pipe_lingers_no_exit_check executable \
        "<command_termination_timeout>2</command_termination_timeout><check_exit_code>0</check_exit_code>" pipe_lingers.py
    pipe_function pipe_lingers_no_exit_check_no_grace executable \
        "<command_termination_timeout>0</command_termination_timeout><check_exit_code>0</check_exit_code>" pipe_lingers.py
    pipe_function pipe_exit_after_a_moment executable "<command_termination_timeout>0</command_termination_timeout>" pipe_exit_after_a_moment.py
    pipe_function pipe_stray_stdout_then_stderr executable \
        "<check_exit_code>0</check_exit_code><stderr_reaction>throw</stderr_reaction>" pipe_stray_stdout_then_stderr.py
    pipe_function pipe_pool_lingers executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>2</command_termination_timeout>" pipe_pool_lingers.py
    pipe_function pipe_pool_lingers_no_exit_check executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>2</command_termination_timeout><check_exit_code>0</check_exit_code>" pipe_pool_lingers.py
    pipe_function pipe_pool_lingers_no_grace executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>0</command_termination_timeout>" pipe_pool_lingers.py
    pipe_function pipe_pool_short_answer executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>20</command_termination_timeout>" pipe_pool_short_answer.py
    pipe_function pipe_pool_close_stdout_wait_stdin executable_pool \
        "<pool_size>1</pool_size><command_termination_timeout>20</command_termination_timeout>" pipe_pool_answer_close_stdout_wait_stdin.py
} | shm_functions

echo "--- stderr written on the way out fails the query, with or without the exit check"
# The command closes its stdout, outlasts the drain that follows it and only then writes its line:
# the bounded wait that reaps it is the last stretch in which it can write, and \`stderr_reaction\` has
# nothing to do with \`check_exit_code\`.
shm_local "
    SELECT pipe_on_the_way_out(1);
    SELECT pipe_on_the_way_out_no_exit_check(1);
"
shm_output_contains "complaining on the way out"

echo "--- a command that outlives its termination timeout on its way out is waited for"
# Its exit status is waited for without a bound, so it passes \`check_exit_code\`; with the check off
# the status is not wanted, and the same command answers normally.
shm_local "
    SELECT pipe_lingers(1);
    SELECT pipe_lingers_no_exit_check(1);
"

echo "--- a zero termination timeout does not turn the wait for the exit status into one probe"
# The command exits a moment after its output ends. Zero means \"signal at once\" for a command being
# discarded; for the wait for an exit status it means no bound, and the query succeeds every time.
shm_local "
    SELECT pipe_exit_after_a_moment(1);
    SELECT pipe_exit_after_a_moment(2);
    SELECT pipe_exit_after_a_moment(3);
"

echo "--- a stray line on stdout after the rows does not hide the stderr after it"
# Without the exit check, a server that closed the command's stdout as soon as it had the rows would
# have the stray write kill the command, its diagnostic unwritten. The line is read and dropped.
shm_local "
    SELECT pipe_stray_stdout_then_stderr(1);
"
shm_output_contains "late complaint"

echo "--- with no grace and no exit check, a lingering command is not waited for"
shm_local "
    SELECT pipe_lingers_no_exit_check_no_grace(1);
"

echo "--- a pooled worker that closed its stdout and lingers fails the query under the exit check"
# It cannot go back to the pool, so its exit status is read then - and there is none within the
# budget, which is not a passing one. Without the check the answer stands, and the worker is replaced.
shm_local "
    SELECT pipe_pool_lingers(1);
    SELECT pipe_pool_lingers_no_exit_check(1);
    SELECT pipe_pool_lingers_no_exit_check(2);
"
shm_output_contains "closed its stdout but did not exit within command_termination_timeout"

echo "--- with no grace, such a worker fails the query at once and frees its slot"
shm_local "
    SELECT pipe_pool_lingers_no_grace(1);
    SELECT pipe_pool_lingers_no_grace(2);
"
shm_output_contains "did not exit within command_termination_timeout"

echo "--- a pooled worker that is not going back to the pool sees the end of its stdin first"
# Both commands close their stdout and read their stdin to the end before exiting, the way a pooled
# command exits. One answered short and fails the query for that - not for an exit status it could
# only have given after \`command_termination_timeout\`; the other answered in full and passes.
shm_local "
    SELECT pipe_pool_short_answer(number) FROM numbers(3);
    SELECT pipe_pool_close_stdout_wait_stdin(1);
    SELECT pipe_pool_close_stdout_wait_stdin(2);
"
shm_output_contains "did not exit within command_termination_timeout"
