#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: the commands are waited for through `/proc`

# The worker answers, closes its stdout and waits for both of its inputs to reach EOF. A hung-up
# stdout is a worker that cannot serve anyone else, so it is discarded - and with `check_exit_code`
# the query is entitled to its exit status, which means the server has to let it exit. Closing only
# its stdin would leave it reading its second input: the wait would run out and the query would fail
# for an exit code it could have had. `command_termination_timeout` is short on purpose: the answer
# has to come back without anything waiting that budget out.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

echo -n | shm_functions

shm_local "
    CREATE TABLE t (value String)
        ENGINE = ExecutablePool('pipe_pool_multiple_inputs_closing_stdout.py', 'TabSeparated', (SELECT 1), (SELECT 2))
        SETTINGS send_chunk_header = 1, pool_size = 1, check_exit_code = 1, command_termination_timeout = 3;
    SELECT * FROM t;
"
