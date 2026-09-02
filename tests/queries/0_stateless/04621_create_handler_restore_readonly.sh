#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `BACKUP` and `RESTORE` run under the `readonly = 2` mode that the HTTP execution path sets for safe
# methods (`BackupsWorker` rejects them only under the strict, user-set `readonly = 1`), so the runtime
# `readonly` enforcement cannot fence them off. But they have durable side effects, and HTTP requires safe
# methods to be side-effect-free: a handler declared for GET is also served for HEAD, where the suppressed
# response body would hide the effect entirely. So a BACKUP / RESTORE handler must list only mutating
# methods (POST, PUT, DELETE) - both at CREATE and at ALTER time.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hr_${DB}"
P="/hr_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_restore\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_backup\`"
}
trap cleanup EXIT
cleanup

echo "=== a RESTORE handler with the default (GET) methods is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_restore\` URL '${P}/restore' AS RESTORE TABLE ${DB}.t FROM Disk('backups', '${DB}.zip')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a BACKUP handler mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_backup\` URL '${P}/backup' METHODS (GET, POST) AS BACKUP TABLE ${DB}.t TO Disk('backups', '${DB}.zip')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a RESTORE handler with only mutating methods is accepted ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_restore\` URL '${P}/restore' METHODS (POST) AS RESTORE TABLE ${DB}.t FROM Disk('backups', '${DB}.zip')"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '${H}_restore'"

echo "=== the RESTORE handler is not served over GET or HEAD ==="
# The handler does not declare GET, so neither GET nor the implicit HEAD alias routes to it: a liveness
# probe or crawler hitting the URL gets 404 and never triggers the restore.
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/restore"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' --head "${BASE}${P}/restore"

echo "=== the same URL over POST does reach query execution ==="
# The backup file does not exist, so execution fails and the server answers with an error status -
# which proves the request was routed to the query, unlike the 404 of the safe methods above.
code=$(${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}' -X POST "${BASE}${P}/restore")
if [ "$code" = "404" ]; then echo "not routed"; else echo "routed"; fi

echo "=== ALTER cannot add a safe method to a RESTORE handler ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_restore\` METHODS (GET, POST)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== ALTER cannot swap a safe-method handler's query to BACKUP ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_backup\` URL '${P}/backup' METHODS (GET) AS SELECT 1"
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_backup\` AS BACKUP TABLE ${DB}.t TO Disk('backups', '${DB}.zip')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a genuinely mutating query over GET-only methods is still rejected ==="
# (`INSERT ... SELECT` rather than `INSERT ... VALUES`: inline `VALUES` data is not part of the AST,
# so it cannot appear inside a handler definition and fails to parse before the method gate runs.)
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_insert\` URL '${P}/insert' METHODS (GET) AS INSERT INTO ${DB}.t SELECT 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
