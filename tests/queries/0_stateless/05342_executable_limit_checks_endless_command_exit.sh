#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/endless_command_exit_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

# Killed by `SIGPIPE` itself, and through a shell that reports it as exit code 141.
printf '#!/usr/bin/env bash\nexec yes row\n' > "${SCRIPTS_DIR}/endless.sh"
printf '#!/usr/bin/env bash\nyes row\n' > "${SCRIPTS_DIR}/endless_child.sh"
chmod +x "${SCRIPTS_DIR}/endless.sh" "${SCRIPTS_DIR}/endless_child.sh"

# `LIMIT` abandons the output before EOF. Draining forever would keep the producer alive;
# its stdout is closed after the stray-output allowance, and the `SIGPIPE` that causes is not
# the command's failure even with `check_exit_code`. The long termination grace must not delay
# a producer that exits on `SIGPIPE`.
for script in endless.sh endless_child.sh; do
    for reaction in none log; do
        $CLICKHOUSE_LOCAL --query "
            SELECT count() FROM (
                SELECT * FROM executable('$script', 'TabSeparated', 'value String',
                    SETTINGS stderr_reaction = '$reaction', check_exit_code = 1, command_termination_timeout = 86400)
                LIMIT 3)
        " -- --user_scripts_path="$SCRIPTS_DIR"
    done
done
