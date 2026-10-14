#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `readonly = 2` mode that the HTTP execution path sets for safe methods does not block an ALTER of a
# session temporary table: `InterpreterAlterQuery::executeToTable` resolves the (external-first) storage and
# rewrites the AST database to the resolved temporary database *before* the access check, and access to
# temporary tables is granted unconditionally (see `ContextAccess`). So an `ALTER` whose target table is not
# qualified with a database may hit a session temporary table and must list only mutating methods
# (POST, PUT, DELETE) - both at CREATE and at ALTER HANDLER time. `ALTER TEMPORARY TABLE` parses to the same
# unqualified statement. A database-qualified target can never be a temporary table, so such handlers may
# still mix safe and mutating methods - the runtime `readonly` enforcement rejects the safe-method
# invocations.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hat_${DB}"
P="/hat_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_alt\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_qual\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_tmp\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_sel\`"
}
trap cleanup EXIT
cleanup

echo "=== an ALTER handler with an unqualified target mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_alt\` URL '${P}/alt' METHODS (GET, POST) AS ALTER TABLE t04837 ADD COLUMN y UInt8" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an ALTER TEMPORARY TABLE handler mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_alt\` URL '${P}/alt' METHODS (GET, PUT) AS ALTER TEMPORARY TABLE t04837 ADD COLUMN y UInt8" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an unqualified ALTER DELETE handler with the default methods (GET only) is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_alt\` URL '${P}/alt' AS ALTER TABLE t04837 DELETE WHERE 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a database-qualified ALTER handler may still mix methods; GET is rejected at invocation by readonly ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t04837 (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_qual\` URL '${P}/qual' METHODS (GET, POST) AS ALTER TABLE ${DB}.t04837 ADD COLUMN y UInt8"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/qual" | grep -o "READONLY" | head -1
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/qual"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.columns WHERE database = '${DB}' AND table = 't04837' ORDER BY name"

echo "=== an unqualified ALTER handler with only mutating methods alters the session temporary table over POST ==="
SESSION="${DB}_session_04837"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_tmp\` URL '${P}/tmp' METHODS (POST) AS ALTER TABLE t04837_tmp ADD COLUMN y UInt8"
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "CREATE TEMPORARY TABLE t04837_tmp (x UInt64)"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/tmp?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "DESCRIBE TABLE t04837_tmp" | cut -f1

echo "=== the handler is not served over GET or HEAD ==="
# The handler does not declare GET, so neither GET nor the implicit HEAD alias routes to it: a liveness
# probe or crawler hitting the URL with a session_id can never mutate that session's temporary tables.
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/tmp?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' --head "${BASE}${P}/tmp?session_id=${SESSION}"

echo "=== ALTER HANDLER cannot swap a safe-method handler's query to an unqualified ALTER ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_sel\` URL '${P}/sel' AS SELECT 1"
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_sel\` AS ALTER TABLE t04837 ADD COLUMN y UInt8" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t04837"
