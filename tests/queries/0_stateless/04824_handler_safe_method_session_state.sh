#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Session-mutating statements (`SET`, `SET ROLE`, `USE`, `BEGIN` / `COMMIT` / `ROLLBACK` /
# `SET TRANSACTION SNAPSHOT`) run under the `readonly = 2` mode that the HTTP execution path sets for safe
# methods, so the runtime `readonly` enforcement cannot fence them off. But they change session or transaction
# state that persists across requests when `session_id` is in use, and HTTP requires safe methods to be
# side-effect-free: a handler declared for GET is also served for HEAD, where the suppressed response body
# would hide the effect entirely. So such a handler must list only mutating methods (POST, PUT, DELETE) -
# both at CREATE and at ALTER time.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hs_${DB}"
P="/hs_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_set\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_use\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_commit\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_select\`"
}
trap cleanup EXIT
cleanup

echo "=== a SET handler with the default (GET) methods is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_set\` URL '${P}/set' AS SET max_block_size = 65411" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a SET ROLE handler with the default (GET) methods is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_set\` URL '${P}/setrole' AS SET ROLE NONE" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a USE handler mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_use\` URL '${P}/use' METHODS (GET, POST) AS USE system" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== transaction control handlers with safe methods are rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_commit\` URL '${P}/begin' AS BEGIN TRANSACTION" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_commit\` URL '${P}/commit' AS COMMIT" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_commit\` URL '${P}/rollback' AS ROLLBACK" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a SET handler with only mutating methods is accepted ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_set\` URL '${P}/set' METHODS (POST) AS SET max_block_size = 65411"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '${H}_set'"

echo "=== the SET handler is not served over GET or HEAD ==="
# The handler does not declare GET, so neither GET nor the implicit HEAD alias routes to it: a liveness
# probe or crawler hitting the URL with a session_id can never alter that session's state.
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/set?session_id=${DB}_probe"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' --head "${BASE}${P}/set?session_id=${DB}_probe"

echo "=== over POST with a session_id, the SET persists into the session ==="
SESSION="${DB}_session"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/set?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "SELECT getSetting('max_block_size')"

echo "=== ALTER cannot swap a safe-method handler's query to SET ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_select\` URL '${P}/select' AS SELECT 1"
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_select\` AS SET max_block_size = 65411" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
