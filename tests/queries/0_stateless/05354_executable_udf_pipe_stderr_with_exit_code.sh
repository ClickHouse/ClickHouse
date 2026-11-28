#!/usr/bin/env bash

# Under `stderr_reaction` `throw` with `check_exit_code`, a command that writes its diagnostic on its
# way out and then exits with an error fails the query for the exit status - and the error carries
# what it wrote on stderr. The line is written after the command closed its stdout and paused, so it
# is read only by the wait for its exit, after the check that gives stderr priority over the exit
# status has already passed; it must not be lost for that.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    echo "<function><type>executable</type><name>pipe_complains_and_fails</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><stderr_reaction>throw</stderr_reaction>"
    echo "<command>pipe_udf.py --close-stdout --pause 0.3 --stderr-on-the-way-out --exit-code 1</command></function>"
} | shm_functions

shm_local "SELECT pipe_complains_and_fails(1);"
shm_output_contains "complaining on the way out"
