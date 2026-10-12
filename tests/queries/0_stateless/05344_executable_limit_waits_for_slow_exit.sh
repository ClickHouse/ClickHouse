#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/slow_exit_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

# Writes a few rows, then takes longer to exit than `command_termination_timeout` gives it.
printf '#!/usr/bin/env bash\nfor i in $(seq 1 100); do echo row; done\nsleep 2\nexit 0\n' > "${SCRIPTS_DIR}/slow_exit.sh"
chmod +x "${SCRIPTS_DIR}/slow_exit.sh"

# `LIMIT` stops reading before the command's output ends. The exit status of an `executable` command
# is waited for without a bound all the same, as when its output is read to the end: a command that
# has stopped writing and exits successfully, however long it takes, passes.
$CLICKHOUSE_LOCAL --query "
    SELECT count() FROM (
        SELECT * FROM executable('slow_exit.sh', 'TabSeparated', 'value String',
            SETTINGS check_exit_code = 1, command_termination_timeout = 1)
        LIMIT 10)
" -- --user_scripts_path="$SCRIPTS_DIR"

# A producer that goes on writing, slowly: it would take hours to write the extra output after which
# its stdout is closed. It is closed once `command_termination_timeout` has passed instead, and the
# producer dies on its next write - on `SIGPIPE`, which is its exit status.
printf '#!/usr/bin/env bash\nwhile true; do echo row; sleep 0.1; done\n' > "${SCRIPTS_DIR}/ticker.sh"
chmod +x "${SCRIPTS_DIR}/ticker.sh"

$CLICKHOUSE_LOCAL --query "
    SELECT count() FROM (
        SELECT * FROM executable('ticker.sh', 'TabSeparated', 'value String',
            SETTINGS check_exit_code = 1, command_termination_timeout = 1)
        LIMIT 3)
" -- --user_scripts_path="$SCRIPTS_DIR" 2>&1 | grep -o 'CHILD_WAS_NOT_EXITED_NORMALLY' | head -1
