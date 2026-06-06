#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-client detects whether it was invoked under an AI coding agent by
# inspecting environment variables, and reports it in ClientInfo.client_agent,
# which is exposed via system.query_log.

query_id_claude="04305_claude_${CLICKHOUSE_DATABASE}"
query_id_generic="04305_generic_${CLICKHOUSE_DATABASE}"
query_id_none="04305_none_${CLICKHOUSE_DATABASE}"

# A known agent marker is mapped to its canonical id.
CLAUDECODE=1 $CLICKHOUSE_CLIENT --query_id "$query_id_claude" --query "SELECT 1 FORMAT Null"
# The generic AGENT variable is reported verbatim.
AGENT="my-custom-agent" $CLICKHOUSE_CLIENT --query_id "$query_id_generic" --query "SELECT 1 FORMAT Null"
# Without any agent marker the field is empty.
$CLICKHOUSE_CLIENT --query_id "$query_id_none" --query "SELECT 1 FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"

get_agent() {
    $CLICKHOUSE_CLIENT --query "
        SELECT client_agent
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND query_id = '$1'
        LIMIT 1
    "
}

echo -n "claude: "; get_agent "$query_id_claude"
echo -n "generic: "; get_agent "$query_id_generic"
echo -n "none: "; get_agent "$query_id_none"
