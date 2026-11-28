#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/endless_command_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

# Produces rows forever. What ends it is the closing of its stdout, not a request it never reads.
printf '#!/usr/bin/env bash\nyes row\n' > "${SCRIPTS_DIR}/endless.sh"
chmod +x "${SCRIPTS_DIR}/endless.sh"

# The query takes three rows and is done. With the exit code not checked, nothing about the
# command's exit is waited for - its stdout is closed and its next write kills it with SIGPIPE - and
# the query returns at once, whether or not its stderr is observed. Draining the command's stdout
# while waiting for it to exit would keep it alive, and the query waiting, for the whole
# `command_termination_timeout`: set far beyond the time limit of the test, that is a hang.
for reaction in none log; do
    $CLICKHOUSE_LOCAL --query "
        SELECT * FROM executable('endless.sh', 'TabSeparated', 'value String',
            SETTINGS stderr_reaction = '$reaction', check_exit_code = 0, command_termination_timeout = 86400)
        LIMIT 3
    " -- --user_scripts_path="$SCRIPTS_DIR"
done
