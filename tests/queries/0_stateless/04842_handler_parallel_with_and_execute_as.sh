#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `statement1 PARALLEL WITH statement2` and `EXECUTE AS <user> <statement>` are composite wrappers: their own
# query kind says nothing about what runs, and both interpreters execute the wrapped statements under a copy of
# the handler's context (which keeps `readonly`). So the safe-method fences must follow the wrapped statements:
# otherwise a handler declared for GET could reach a `BACKUP` (a durable side effect that `readonly = 2` does not
# block) or a statement that mutates a session temporary table, and a GET declaration is also served for HEAD,
# where the suppressed response body would hide the effect entirely. Conversely, a composite of read-only
# statements must not be forced to declare a mutating method.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hpw_${DB}"
P="/hpw_${DB}"

cleanup() {
    for suffix in backup restore setting tmp ok sel exec; do
        $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_${suffix}\`"
    done
}
trap cleanup EXIT
cleanup

echo "=== a PARALLEL WITH handler with a BACKUP child mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_backup\` URL '${P}/backup' METHODS (GET, POST) AS BACKUP TABLE ${DB}.t04842 TO Disk('backups', '${DB}_a.zip') PARALLEL WITH BACKUP TABLE ${DB}.u04842 TO Disk('backups', '${DB}_b.zip')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a RESTORE child behind a read-only first statement is found too ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_restore\` URL '${P}/restore' METHODS (GET, POST) AS SELECT 1 PARALLEL WITH RESTORE TABLE ${DB}.t04842 FROM Disk('backups', '${DB}_a.zip')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a session-mutating SET child is found too ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_setting\` URL '${P}/setting' METHODS (GET, POST) AS SELECT 1 PARALLEL WITH SET max_threads = 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an unqualified INSERT child, which may target a session temporary table, is found too ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_tmp\` URL '${P}/tmp' METHODS (GET, POST) AS SELECT 1 PARALLEL WITH INSERT INTO t04842 SELECT 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== the same BACKUP composite with only mutating methods is accepted ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_ok\` URL '${P}/ok' METHODS (POST) AS BACKUP TABLE ${DB}.t04842 TO Disk('backups', '${DB}_a.zip') PARALLEL WITH BACKUP TABLE ${DB}.u04842 TO Disk('backups', '${DB}_b.zip')"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '${H}_ok'"

echo "=== it is not served over GET or HEAD ==="
# The handler does not declare GET, so neither GET nor the implicit HEAD alias routes to it: a liveness probe
# or crawler hitting the URL gets 404 and never triggers the backups.
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/ok"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' --head "${BASE}${P}/ok"

echo "=== the same URL over POST does reach query execution ==="
# The tables do not exist, so execution fails and the server answers with an error status - which proves the
# request was routed to the query, unlike the 404 of the safe methods above.
code=$(${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}' -X POST "${BASE}${P}/ok")
if [ "$code" = "404" ]; then echo "not routed"; else echo "routed"; fi

echo "=== a composite of read-only statements needs no mutating method ==="
# The wrapper kind alone is classified as modifying, so following the wrapped statements is what makes this
# handler creatable with the default (GET) methods.
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_sel\` URL '${P}/sel' AS SELECT 1 PARALLEL WITH SELECT 2"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '${H}_sel'"

echo "=== ALTER HANDLER cannot swap a safe-method handler's query to a composite with a BACKUP ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_sel\` AS SELECT 1 PARALLEL WITH BACKUP TABLE ${DB}.t04842 TO Disk('backups', '${DB}_a.zip')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an EXECUTE AS handler wrapping a BACKUP mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_exec\` URL '${P}/exec' METHODS (GET, POST) AS EXECUTE AS user04842 BACKUP TABLE ${DB}.t04842 TO Disk('backups', '${DB}_a.zip')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an EXECUTE AS handler wrapping an INSERT requires a mutating method ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_exec\` URL '${P}/exec' METHODS (GET) AS EXECUTE AS user04842 INSERT INTO ${DB}.t04842 SELECT 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a bare EXECUTE AS handler, which makes the session run as another user, is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_exec\` URL '${P}/exec' METHODS (GET, POST) AS EXECUTE AS user04842" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an EXECUTE AS handler wrapping a SELECT is accepted with the default methods ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_exec\` URL '${P}/exec' AS EXECUTE AS user04842 SELECT 1"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '${H}_exec'"
