#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# There is a single, non-rewindable HTTP request body. Binding the `_request_body` parameter drains it (see
# `PredefinedQueryHandler::customizeContext`) before the query's input pipeline is built, so a handler whose
# query also takes the body as its own input data (a plain `INSERT`, or an `INSERT ... SELECT` reading from
# `input`) would find the stream at EOF and silently insert nothing. Such a handler is rejected at creation.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hbc_${DB}"
P="/hbc_${DB}"

cleanup() {
    for suffix in conflict raw input; do
        $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`${H}_${suffix}\`"
    done
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.strings04871 (s String) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.numbers04871 (x UInt64) ENGINE = Memory"

echo "=== a handler combining input() with the _request_body parameter is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_conflict\` URL '${P}/conflict' METHODS (POST) AS INSERT INTO ${DB}.strings04871 SELECT {_request_body:String} FROM input('x UInt64') FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a handler whose only body use is _request_body is accepted and receives the raw body ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_raw\` URL '${P}/raw' METHODS (POST) AS INSERT INTO ${DB}.strings04871 SELECT {_request_body:String}"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/raw" --data-binary 'hello body'
$CLICKHOUSE_CLIENT -q "SELECT s FROM ${DB}.strings04871"

echo "=== ALTER HANDLER cannot swap its query to a conflicting one either ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`${H}_raw\` AS INSERT INTO ${DB}.strings04871 SELECT {_request_body:String} FROM input('x UInt64') FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a handler whose only body use is its own input() is accepted and receives the uploaded rows ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`${H}_input\` URL '${P}/input' METHODS (POST) AS INSERT INTO ${DB}.numbers04871 SELECT x * 2 FROM input('x UInt64') FORMAT TSV"
printf '11\n22\n' | ${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}/input" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${DB}.numbers04871"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.strings04871"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.numbers04871"
