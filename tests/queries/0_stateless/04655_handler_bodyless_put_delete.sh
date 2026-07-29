#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A SQL-defined handler that never consumes the request body must be reachable by an ordinary HTTP client:
# requiring `Content-Length` for every `PUT` / `DELETE` would make that class of handlers unusable.

# Base URL for the user-facing HTTP port (no path / no auth: default user).
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/hb_${DB}"
HPUT="hbput_${DB}"
HDEL="hbdel_${DB}"
HPARAM="hbparam_${DB}"

cleanup() {
    local drops=""
    for h in "$HPUT" "$HDEL" "$HPARAM"; do
        drops+="DROP HANDLER IF EXISTS \`$h\`; "
    done
    $CLICKHOUSE_CLIENT -q "${drops}"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPUT\` URL '${P}/put' METHODS (PUT) AS SELECT 1 AS a FORMAT TSV"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HDEL\` URL '${P}/del' METHODS (DELETE) AS SELECT 2 AS a FORMAT TSV"

echo "=== a lengthless non-chunked PUT to a handler that does not read the body succeeds ==="
# `-X PUT` with no data sends neither Content-Length nor chunked Transfer-Encoding.
${CLICKHOUSE_CURL} -sS -X PUT "${BASE}${P}/put"

echo "=== a lengthless non-chunked DELETE to a handler that does not read the body succeeds ==="
${CLICKHOUSE_CURL} -sS -X DELETE "${BASE}${P}/del"

echo "=== a handler reading _request_body still requires the length up front ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPARAM\` URL '${P}/param' METHODS (PUT) AS SELECT {_request_body:String} AS a FORMAT TSV"
${CLICKHOUSE_CURL} -sS -X PUT -I "${BASE}${P}/param" | grep -c '411 Length Required'

echo "=== and it returns the body when the length is given ==="
# An explicit non-form content type: `application/x-www-form-urlencoded` (curl's default for `--data-binary`)
# would make the handler layer parse the body as a form instead of leaving it to `_request_body`.
${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: text/plain' --data-binary 'hello' "${BASE}${P}/param"
