#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler may declare `_request_body` alongside form-bindable parameters. Form parsing consumes the request
# body, and `_request_body` is bound only afterwards, so without special care it would observe EOF and bind an
# empty string. A copy of the raw body is preserved before the body is parsed as a form, so both contracts
# hold: `_request_body` receives the raw, unparsed body and the form fields bind their parameters.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/h_${DB}"
HBOTH="hboth_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HBOTH\`"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HBOTH\` URL '${P}/both' METHODS (POST, PUT) AS SELECT {id:UInt64} * 2 AS r, {_request_body:String} AS body FORMAT TSV"

echo "=== a urlencoded body binds the parameter and _request_body gets the raw body ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21' "${BASE}${P}/both"
${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'id=3&other=x' "${BASE}${P}/both"

echo "=== a multipart body binds the parameter and _request_body gets the raw multipart envelope ==="
${CLICKHOUSE_CURL} -sS -X POST -F 'param_id=7' "${BASE}${P}/both" | grep -c 'name="param_id"'
${CLICKHOUSE_CURL} -sS -X POST -F 'param_id=7' "${BASE}${P}/both" | cut -f 1

echo "=== a non-form body still binds _request_body raw, with the parameter from the URL query string ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/json' --data-binary '{"k":1}' "${BASE}${P}/both?param_id=5"

echo "=== a form-parsed parameter present in both the URL and the body still takes its value from the URL ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21' "${BASE}${P}/both?param_id=9"

echo "=== _request_body supplied in the URL wins over the raw body, and form fields still bind ==="
${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data-binary 'param_id=21' "${BASE}${P}/both?param__request_body=url_wins"
