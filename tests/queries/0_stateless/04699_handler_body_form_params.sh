#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query parameters of a handler can be supplied in the request body: an `application/x-www-form-urlencoded`
# body and the fields of a `multipart/form-data` body bind `{name:Type}` parameters the same way as URL
# query-string parameters, on the body-carrying methods POST, PUT and DELETE. A body parsed as a form is
# consumed by the handler layer: it never binds `_request_body` and is never fed to the query. Body fields
# bind only declared parameters - unlike URL parameters they are never treated as settings.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/h_${DB}"
HID="hform_${DB}"
HRAW="hraw_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HID\`"
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HRAW\`"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HID\` URL '${P}/id' METHODS (GET, POST, PUT, DELETE) AS SELECT {id:UInt64} * 2 AS r FORMAT TSV"

echo "=== POST with a urlencoded body binds the parameter, param_-prefixed or bare ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21' "${BASE}${P}/id"
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'id=21' "${BASE}${P}/id"

echo "=== PUT and DELETE with a urlencoded body bind the parameter too ==="
${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=3' "${BASE}${P}/id"
${CLICKHOUSE_CURL} -sS -X DELETE -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=4' "${BASE}${P}/id"

echo "=== a parameter present in both the URL and the body takes its value from the URL ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21' "${BASE}${P}/id?param_id=5"

echo "=== undeclared body fields are ignored, they are not settings ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21&no_such_parameter=1' "${BASE}${P}/id"

echo "=== a urlencoded body without the field leaves the parameter unset ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'other=1' "${BASE}${P}/id" | grep -o "UNKNOWN_QUERY_PARAMETER" | head -1

echo "=== multipart form fields bind the parameter, over POST and DELETE ==="
${CLICKHOUSE_CURL} -sS -X POST -F 'param_id=7' "${BASE}${P}/id"
${CLICKHOUSE_CURL} -sS -X DELETE -F 'param_id=8' "${BASE}${P}/id"

echo "=== the GET control still binds from the URL query string ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/id?param_id=1"

echo "=== a handler whose only body use is _request_body still gets the raw body, not form parsing ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HRAW\` URL '${P}/raw' METHODS (POST) AS SELECT {_request_body:String} AS r FORMAT TSV"
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21' "${BASE}${P}/raw"
