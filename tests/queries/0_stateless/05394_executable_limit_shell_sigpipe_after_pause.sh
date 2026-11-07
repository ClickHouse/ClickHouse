#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/sigpipe_after_pause_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

# A producer behind a shell that pauses for longer than `command_termination_timeout`: its
# abandoned stdout is closed during the pause, its next write dies on `SIGPIPE`, and the shell
# reports that as exit code 141. Nothing that was read before the close tells this apart from a
# command exiting with 141 on its own, and the ambiguity is resolved in favour of the `LIMIT`:
# the query succeeds even with `check_exit_code`.
printf '#!/usr/bin/env bash\nseq 1 100\nsleep 2\nseq 1 100\n' > "${SCRIPTS_DIR}/pause_then_write.sh"
chmod +x "${SCRIPTS_DIR}/pause_then_write.sh"

$CLICKHOUSE_LOCAL --query "
    SELECT count() FROM (
        SELECT * FROM executable('pause_then_write.sh', 'TabSeparated', 'value String',
            SETTINGS check_exit_code = 1, command_termination_timeout = 1)
        LIMIT 10)
" -- --user_scripts_path="$SCRIPTS_DIR"
