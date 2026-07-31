#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler whose query reads the HTTP request body (`_request_body`) must allow at least one
# body-carrying method (POST, PUT or DELETE): a safe method such as GET never supplies a body,
# so the query would silently bind an empty one. Creation and ALTER both enforce this.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/h_${DB}"
HBODY="hbody_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HBODY\`"
}
trap cleanup EXIT
cleanup

echo "=== a _request_body handler with the default GET method is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HBODY\` URL '${P}/body' AS SELECT {_request_body:String} AS r FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a _request_body handler with only GET is rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HBODY\` URL '${P}/body' METHODS (GET) AS SELECT {_request_body:String} AS r FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a _request_body handler with a body-carrying method is accepted and works ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HBODY\` URL '${P}/body' METHODS (GET, POST) AS SELECT {_request_body:String} AS r FORMAT TSV"
# An explicit non-form content type: `application/x-www-form-urlencoded` (curl's default for `--data-binary`)
# would make the handler layer parse the body as a form instead of leaving it to `_request_body`.
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: text/plain' --data-binary 'hello' "${BASE}${P}/body"

echo "=== ALTER cannot narrow the methods of a _request_body handler to safe ones ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`$HBODY\` METHODS (GET)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== ALTER cannot introduce _request_body into a GET-only handler ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`$HBODY\` METHODS (GET) AS SELECT 1" && echo "narrowed to GET with a body-free query"
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`$HBODY\` AS SELECT {_request_body:String} AS r FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
