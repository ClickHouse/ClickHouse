#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-client / clickhouse-local detect whether they were invoked under an AI coding agent
# by inspecting environment variables, and report it in ClientInfo.client_agent, which is exposed
# via system.query_log.

# Start each case from a clean slate so the test is deterministic regardless of
# the environment the test runner itself is invoked from (it might already be an agent).
CLEAN_ENV="env -u CLAUDECODE -u CLAUDE_CODE -u CURSOR_TRACE_ID -u CURSOR_AGENT \
    -u CURSOR_EXTENSION_HOST_ROLE -u GEMINI_CLI -u CODEX_SANDBOX -u CODEX_CI -u CODEX_THREAD_ID \
    -u ANTIGRAVITY_AGENT -u AUGMENT_AGENT -u CLINE_ACTIVE -u OPENCODE_CLIENT -u TRAE_AI_SHELL_ID \
    -u GOOSE_TERMINAL -u REPL_ID -u COPILOT_MODEL -u COPILOT_ALLOW_ALL -u COPILOT_GITHUB_TOKEN -u AGENT"

query_id_claude="04305_claude_${CLICKHOUSE_DATABASE}"
query_id_cursor="04305_cursor_${CLICKHOUSE_DATABASE}"
query_id_generic="04305_generic_${CLICKHOUSE_DATABASE}"
query_id_none="04305_none_${CLICKHOUSE_DATABASE}"
query_id_local="04305_local_${CLICKHOUSE_DATABASE}"

# A known agent marker is mapped to its canonical id.
$CLEAN_ENV CLAUDECODE=1 $CLICKHOUSE_CLIENT --query_id "$query_id_claude" --query "SELECT 1 FORMAT Null"
$CLEAN_ENV CURSOR_TRACE_ID=test $CLICKHOUSE_CLIENT --query_id "$query_id_cursor" --query "SELECT 1 FORMAT Null"
# The generic AGENT variable is reported verbatim.
$CLEAN_ENV AGENT="my-custom-agent" $CLICKHOUSE_CLIENT --query_id "$query_id_generic" --query "SELECT 1 FORMAT Null"
# Without any agent marker the field is empty.
$CLEAN_ENV $CLICKHOUSE_CLIENT --query_id "$query_id_none" --query "SELECT 1 FORMAT Null"

# clickhouse-local detects the agent too, and the value must be preserved when its query is
# forwarded to a remote server (the secondary query runs with the LOCAL interface client info).
$CLEAN_ENV CLAUDECODE=1 $CLICKHOUSE_LOCAL --query_id "$query_id_local" \
    --query "SELECT * FROM remote('${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}', system.one) FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

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
echo -n "cursor: "; get_agent "$query_id_cursor"
echo -n "generic: "; get_agent "$query_id_generic"
echo -n "none: "; get_agent "$query_id_none"

# The secondary (remote) query carries the initiating clickhouse-local query id as initial_query_id.
echo -n "local->remote: "
$CLICKHOUSE_CLIENT --query "
    SELECT client_agent
    FROM system.query_log
    WHERE initial_query_id = '$query_id_local'
        AND is_initial_query = 0
        AND type = 'QueryFinish'
    LIMIT 1
"
