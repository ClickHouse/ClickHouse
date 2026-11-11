#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `readonly = 2` mode that the HTTP execution path sets for safe methods does not block mutations of an
# *existing* session temporary table: access to temporary tables is granted unconditionally (see
# `ContextAccess`), and `00543_access_to_temporary_table_in_readonly_mode` pins `INSERT` into and
# `DROP TEMPORARY TABLE` of one under `readonly = 2`. A temporary table is a session object that does not
# exist when the handler is created, so the validation is over query shapes and fails close: an `INSERT`
# whose target table is not qualified with a database, a `DROP TEMPORARY TABLE`, and a `DROP TABLE` /
# `TRUNCATE TABLE` of an unqualified table all may hit a session temporary table, so such a handler must
# list only mutating methods (POST, PUT, DELETE) - both at CREATE and at ALTER time. A database-qualified
# target can never be a temporary table, so those handlers may still mix safe and mutating methods - the
# runtime `readonly` enforcement rejects the safe-method invocations.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="htm_${DB}"
P="/htm_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_ins\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_qual\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_tmp\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_drop\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_sel\`"
}
trap cleanup EXIT
cleanup

echo "=== an INSERT handler with an unqualified target mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_ins\` URL '${P}/ins' METHODS (GET, POST) AS INSERT INTO t04828 SELECT 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a DROP TEMPORARY TABLE handler mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_ins\` URL '${P}/ins' METHODS (GET, POST) AS DROP TEMPORARY TABLE t04828" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a DROP TABLE handler with an unqualified target mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_ins\` URL '${P}/ins' METHODS (GET, DELETE) AS DROP TABLE t04828" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a TRUNCATE TABLE handler with an unqualified target mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_ins\` URL '${P}/ins' METHODS (GET, POST) AS TRUNCATE TABLE t04828" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a multi-table DROP with any unqualified target mixing GET with a mutating method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_ins\` URL '${P}/ins' METHODS (GET, POST) AS DROP TABLE ${DB}.t04828, t04828" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a database-qualified INSERT ... SELECT handler may still mix methods; GET is rejected at invocation by readonly ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t04828 (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_qual\` URL '${P}/qual' METHODS (GET, POST) AS INSERT INTO ${DB}.t04828 SELECT 1"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/qual" | grep -o "READONLY" | head -1
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/qual"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${DB}.t04828"

echo "=== an unqualified INSERT handler with only mutating methods mutates the session temporary table over POST ==="
SESSION="${DB}_session"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_tmp\` URL '${P}/tmp' METHODS (POST) AS INSERT INTO t04828_tmp SELECT 42"
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "CREATE TEMPORARY TABLE t04828_tmp (x UInt64)"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/tmp?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "SELECT * FROM t04828_tmp"

echo "=== the handler is not served over GET or HEAD ==="
# The handler does not declare GET, so neither GET nor the implicit HEAD alias routes to it: a liveness
# probe or crawler hitting the URL with a session_id can never mutate that session's temporary tables.
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/tmp?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' --head "${BASE}${P}/tmp?session_id=${SESSION}"

echo "=== an unqualified DROP TABLE handler with only mutating methods drops the session temporary table over POST ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_drop\` URL '${P}/drop' METHODS (POST) AS DROP TABLE t04828_tmp"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/drop?session_id=${SESSION}"
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "EXISTS TEMPORARY TABLE t04828_tmp"

echo "=== ALTER cannot swap a safe-method handler's query to an unqualified INSERT ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_sel\` URL '${P}/sel' AS SELECT 1"
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_sel\` AS INSERT INTO t04828 SELECT 1" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t04828"
