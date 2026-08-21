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
    for suffix in backup restore setting tmp ok sel exec body_pw body_exec body_input body_plain; do
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
# or crawler hitting the URL gets the generic not-found response and never triggers the backups. The status code
# alone cannot prove that: a request routed to the query would fail too, and the exception may map to the same
# 404 status (`UNKNOWN_TABLE` does). So assert on the not-found body, and for the bodyless HEAD response on the
# absence of the exception header that every routed error response carries.
${CLICKHOUSE_CURL} -sS "${BASE}${P}/ok" | grep -o "There is no handle" | head -1
${CLICKHOUSE_CURL} -sS --head "${BASE}${P}/ok" | grep -c "X-ClickHouse-Exception-Code" || true

echo "=== the same URL over POST does reach query execution ==="
# The tables do not exist, so execution fails with an exception - which proves the request was routed to the
# query. The exception may itself map to a 404 status (`UNKNOWN_TABLE`), so routing is detected by the response
# body, not by the status code.
body=$(${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/ok")
if [[ "$body" == *"There is no handle"* ]]; then echo "not routed"; else echo "routed"; fi

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

echo "=== an EXECUTE AS handler needs a mutating method even for a wrapped SELECT (IMPERSONATE is denied under readonly) ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_exec\` URL '${P}/exec' AS EXECUTE AS user04842 SELECT 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== the same EXECUTE AS handler is accepted with a mutating method ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_exec\` URL '${P}/exec' METHODS (POST) AS EXECUTE AS user04842 SELECT 1"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '${H}_exec'"

# Neither wrapper forwards the HTTP request tail to the statements it runs: both re-format them and call
# `executeQuery(String, ...)`, which passes no input buffer. A handler wrapping a body-reading INSERT would
# therefore accept every upload and insert nothing, so it is rejected at creation.
$CLICKHOUSE_CLIENT -q "CREATE TABLE IF NOT EXISTS ${DB}.body04842 (x UInt64) ENGINE = Memory"

echo "=== a PARALLEL WITH handler wrapping an INSERT fed from input() is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_body_pw\` URL '${P}/body_pw' METHODS (POST) AS SELECT 1 PARALLEL WITH INSERT INTO ${DB}.body04842 SELECT x FROM input('x UInt64')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an EXECUTE AS handler wrapping an INSERT fed from input() is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_body_input\` URL '${P}/body_input' METHODS (POST) AS EXECUTE AS user04842 INSERT INTO ${DB}.body04842 SELECT x FROM input('x UInt64')" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== an EXECUTE AS handler wrapping a plain INSERT is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_body_exec\` URL '${P}/body_exec' METHODS (POST) AS EXECUTE AS user04842 INSERT INTO ${DB}.body04842 FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== ALTER HANDLER cannot swap a query to a wrapped body-reading INSERT either ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_exec\` AS EXECUTE AS user04842 INSERT INTO ${DB}.body04842 FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== the unwrapped INSERT handler is accepted and does receive the uploaded rows ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_body_plain\` URL '${P}/body_plain' METHODS (POST) AS INSERT INTO ${DB}.body04842 FORMAT TSV"
printf '11\n22\n' | ${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/body_plain" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${DB}.body04842"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.body04842"
