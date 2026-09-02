#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A SQL-defined handler that never consumes the request body must be reachable by an ordinary HTTP client
# over POST too: a plain `curl -X POST` sends neither a body nor `Content-Length`, so requiring the length
# unconditionally (the historical POST contract of the built-in handlers) would make such handlers unusable.

# Base URL for the user-facing HTTP port (no path / no auth: default user).
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/hbp_${DB}"
HPOST="hbpost_${DB}"
HPARAM="hbpostparam_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HPOST\`; DROP HANDLER IF EXISTS \`$HPARAM\`;"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPOST\` URL '${P}/post' METHODS (POST) AS SELECT 1 AS a FORMAT TSV"

# A lengthless non-chunked body ends only when the connection closes, so a server that waits for it deadlocks
# against a client that waits for the response. Bound the request well below the test timeout, so that
# such a regression shows up as a diff here instead of killing the whole test run.
# Keep it silent and turn a nonzero exit into a stdout line: the harness fails on any stderr before it
# compares the reference, so an error line there would take the place of that diff.
CURL_BOUNDED="${CLICKHOUSE_CURL_COMMAND} -q -s --max-time 30"

echo "=== a lengthless non-chunked POST to a handler that does not read the body succeeds ==="
# `-X POST` with no data sends neither Content-Length nor chunked Transfer-Encoding.
${CURL_BOUNDED} -X POST "${BASE}${P}/post" || echo "curl failed: $?"

echo "=== a handler reading _request_body still requires the length up front ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPARAM\` URL '${P}/param' METHODS (POST) AS SELECT {_request_body:String} AS a FORMAT TSV"
${CLICKHOUSE_CURL} -sS -X POST -I "${BASE}${P}/param" | grep -c '411 Length Required'

echo "=== and it returns the body when the length is given ==="
# An explicit non-form content type: `application/x-www-form-urlencoded` (curl's default for `--data-binary`)
# would make the handler layer parse the body as a form instead of leaving it to `_request_body`.
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: text/plain' --data-binary 'hello' "${BASE}${P}/param"

echo "=== a lengthless urlencoded POST to a non-body-consuming handler still requires the length ==="
# The handler layer itself would parse a declared form body for `{param}` binding, so the length is required
# even though the handler's query does not read the body.
${CLICKHOUSE_CURL} -sS -X POST -I -H 'Content-Type: application/x-www-form-urlencoded' "${BASE}${P}/post" | grep -c '411 Length Required'

echo "=== the built-in dynamic handler keeps the unconditional POST contract ==="
${CLICKHOUSE_CURL} -sS -X POST -I "${BASE}/?query=SELECT%201" | grep -c '411 Length Required'
