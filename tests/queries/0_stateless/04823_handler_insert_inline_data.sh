#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler query is persisted as formatted text, and `ASTInsertQuery::formatImpl` intentionally drops the
# inline payload following `VALUES` or the `FORMAT` clause - a handler defined as
# `AS INSERT INTO t VALUES (1)` would be stored and executed as `INSERT INTO t VALUES` with no rows, silently
# running a different query than the one the user wrote. So an INSERT handler with inline data is rejected -
# both at CREATE and at ALTER time - while the body-fed forms (`... FORMAT TSV`, bare `... VALUES`) and
# `INSERT ... SELECT` stay accepted. The rejection surfaces as a syntax error: the inner-query parser leaves
# the payload unconsumed, so the enclosing CREATE/ALTER HANDLER statement fails to parse (and
# `makeSQLDefinedHandler` keeps an explicit BAD_ARGUMENTS guard behind it, should that ever change).

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hi_${DB}"
P="/hi_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_values\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_format\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_body\`"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t04823 (x UInt32) ENGINE = Memory"

echo "=== an INSERT handler with inline data after VALUES is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_values\` URL '${P}/values' METHODS (POST) AS INSERT INTO ${DB}.t04823 VALUES (1)" 2>&1 | grep -oE "SYNTAX_ERROR|BAD_ARGUMENTS" | head -1

echo "=== an INSERT handler with inline data after FORMAT is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_format\` URL '${P}/format' METHODS (POST) AS INSERT INTO ${DB}.t04823 FORMAT TSV
1" 2>&1 | grep -oE "SYNTAX_ERROR|BAD_ARGUMENTS" | head -1

echo "=== a parenthesized INSERT with inline data is rejected too ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_values\` URL '${P}/values' METHODS (POST) AS (INSERT INTO ${DB}.t04823 VALUES (1))" 2>&1 | grep -oE "SYNTAX_ERROR|BAD_ARGUMENTS" | head -1

echo "=== ALTER cannot swap the query to an INSERT with inline data ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_body\` URL '${P}/body' METHODS (POST) AS INSERT INTO ${DB}.t04823 FORMAT TSV"
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_body\` AS INSERT INTO ${DB}.t04823 VALUES (2)" 2>&1 | grep -oE "SYNTAX_ERROR|BAD_ARGUMENTS" | head -1

echo "=== nothing with inline data got stored ==="
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.handlers WHERE name IN ('${H}_values', '${H}_format')"

echo "=== the body-fed FORMAT form still works and the stored query kept its text ==="
$CLICKHOUSE_CLIENT -q "SELECT query FROM system.handlers WHERE name = '${H}_body'" | sed "s/${DB}/db/g"
${CLICKHOUSE_CURL} -sS -X POST --data-binary $'3\n4' "${BASE}${P}/body"
$CLICKHOUSE_CLIENT -q "SELECT x FROM ${DB}.t04823 ORDER BY x"

echo "=== a bare VALUES form (no inline data) is accepted and body-fed ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_body\` AS INSERT INTO ${DB}.t04823 VALUES"
${CLICKHOUSE_CURL} -sS -X POST --data-binary '(5),(6)' "${BASE}${P}/body"
$CLICKHOUSE_CLIENT -q "SELECT x FROM ${DB}.t04823 ORDER BY x"

echo "=== INSERT ... SELECT with a constant is the supported inline-free alternative ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_body\` AS INSERT INTO ${DB}.t04823 SELECT 7"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/body"
$CLICKHOUSE_CLIENT -q "SELECT x FROM ${DB}.t04823 ORDER BY x"
