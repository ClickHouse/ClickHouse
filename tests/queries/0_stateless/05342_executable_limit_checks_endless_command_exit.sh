#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/endless_command_exit_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

printf '#!/usr/bin/env bash\nexec yes row\n' > "${SCRIPTS_DIR}/endless.sh"
chmod +x "${SCRIPTS_DIR}/endless.sh"

# `LIMIT` abandons the output before EOF. Draining forever would keep the producer alive;
# close its stdout after the stray-output allowance and still check the resulting exit status.
# The long termination grace must not delay a producer that exits on `SIGPIPE`.
for reaction in none log; do
    if $CLICKHOUSE_LOCAL --query "
        SELECT * FROM executable('endless.sh', 'TabSeparated', 'value String',
            SETTINGS stderr_reaction = '$reaction', check_exit_code = 1, command_termination_timeout = 86400)
        LIMIT 3
    " -- --user_scripts_path="$SCRIPTS_DIR" > "$SCRIPTS_DIR/result" 2>&1; then
        cat "$SCRIPTS_DIR/result"
        echo "Expected the command's failing exit status"
        exit 1
    fi
    grep -q '(CHILD_WAS_NOT_EXITED_NORMALLY)' "$SCRIPTS_DIR/result"
    echo "$reaction: exit status checked"
done
