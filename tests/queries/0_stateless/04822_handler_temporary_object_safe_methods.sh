#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW` need only the `CREATE_TEMPORARY_TABLE` access flag,
# which is still allowed under the `readonly = 2` mode that the HTTP execution path sets for safe methods,
# so the runtime `readonly` enforcement cannot fence them off. But the created object lives in the session
# and persists across requests when `session_id` is in use, and HTTP requires safe methods to be
# side-effect-free: a handler declared for GET is also served for HEAD, where the suppressed response body
# would hide the effect entirely. So such a handler must list only mutating methods (POST, PUT, DELETE) -
# both at CREATE and at ALTER time.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="ht_${DB}"
P="/ht_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_table\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_view\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_select\`"
}
trap cleanup EXIT
cleanup

echo "=== a CREATE TEMPORARY TABLE handler with the default (GET) methods is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_table\` URL '${P}/table' AS CREATE TEMPORARY TABLE t04822 (x UInt8)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a CREATE TEMPORARY VIEW handler with the default (GET) methods is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_view\` URL '${P}/view' AS CREATE TEMPORARY VIEW v04822 AS SELECT 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a CREATE TEMPORARY TABLE handler mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_table\` URL '${P}/table' METHODS (GET, POST) AS CREATE TEMPORARY TABLE t04822 (x UInt8)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a CREATE TEMPORARY TABLE handler with only mutating methods is accepted ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_table\` URL '${P}/table' METHODS (POST) AS CREATE TEMPORARY TABLE t04822 (x UInt8)"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '${H}_table'"

echo "=== the handler is not served over GET or HEAD ==="
# The handler does not declare GET, so neither GET nor the implicit HEAD alias routes to it: a liveness
# probe or crawler hitting the URL with a session_id can never create objects in that session.
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/table?session_id=${DB}_probe"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' --head "${BASE}${P}/table?session_id=${DB}_probe"

echo "=== over POST with a session_id, the temporary table persists into the session ==="
SESSION="${DB}_session"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/table?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "SELECT count() FROM t04822"

echo "=== ALTER cannot swap a safe-method handler's query to CREATE TEMPORARY TABLE ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_select\` URL '${P}/select' AS SELECT 1"
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_select\` AS CREATE TEMPORARY TABLE t04822 (x UInt8)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
