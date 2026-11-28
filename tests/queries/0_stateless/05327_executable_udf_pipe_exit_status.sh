#!/usr/bin/env bash

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
    pipe_function pipe_on_the_way_out executable "<stderr_reaction>throw</stderr_reaction>" "pipe_udf.py --close-stdout --pause 1 --stderr-on-the-way-out"
    pipe_function pipe_on_the_way_out_no_exit_check executable \
        "<stderr_reaction>throw</stderr_reaction><check_exit_code>0</check_exit_code>" "pipe_udf.py --close-stdout --pause 1 --stderr-on-the-way-out"
    pipe_function pipe_lingers executable "<command_termination_timeout>1</command_termination_timeout>" "pipe_udf.py --close-stdout --pause 2"
    pipe_function pipe_lingers_no_exit_check executable \
        "<command_termination_timeout>1</command_termination_timeout><check_exit_code>0</check_exit_code>" "pipe_udf.py --close-stdout --pause 2"
    pipe_function pipe_lingers_no_exit_check_no_grace executable \
        "<command_termination_timeout>0</command_termination_timeout><check_exit_code>0</check_exit_code>" "pipe_udf.py --close-stdout --pause 4"
    pipe_function pipe_exit_after_a_moment executable "<command_termination_timeout>0</command_termination_timeout>" "pipe_udf.py --close-stdout --pause 0.3"
    pipe_function pipe_stray_stdout_then_stderr executable \
        "<check_exit_code>0</check_exit_code><stderr_reaction>throw</stderr_reaction>" "pipe_udf.py --pause 0.3 --stray-stdout --stderr-on-the-way-out"
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
shm_output_contains "complaining on the way out"

echo "--- with no grace and no exit check, a lingering command is not waited for"
shm_local "
    SELECT pipe_lingers_no_exit_check_no_grace(1);
"
