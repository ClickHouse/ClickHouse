#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The stored text of a handler belongs to the server, so it is parsed as ClickHouse SQL no matter which
# `dialect` the request asks for. Otherwise `?dialect=prql` (or `kusto`, `promql`, `polyglot`,
# `clickhouse_json`) would hand the stored `SELECT ...` to the wrong parser and make the handler
# uninvokable. Only the query text the *request* owns keeps following the request's `dialect`.
# `04897_handler_request_parser_limits` covers the same contract for the parser limits.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_dialect_${CLICKHOUSE_DATABASE}"
URL="/dialect_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"

echo "=== created ==="
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT 'stored' AS s, {x:UInt64} AS p
" && echo "created"

echo "=== the request dialect is in effect for a request-owned query ==="
# A dialect the request names applies to the text the request sends.
${CLICKHOUSE_CURL} -sS "${BASE}/?dialect=prql" -d "SELECT 1" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED"
${CLICKHOUSE_CURL} -sS "${BASE}/?dialect=kusto" -d "SELECT 1" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED"

echo "=== invoked over HTTP with the same request dialects ==="
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=prql&param_x=1"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=kusto&param_x=2"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=promql&param_x=3"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=polyglot&param_x=4"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=clickhouse_json&param_x=5"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=clickhouse_json&enable_json_ast_dialect=1&param_x=6"

echo "=== request-controlled construction settings still use the request settings ==="
# Only the server-owned stored text escapes the request's parser settings; the `filter` snippet is named
# by the request, so it is still parsed under the request's own parser limits.
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?dialect=prql&max_parser_depth=1&param_x=7&filter=(((((1)))))" 2>&1 | grep -o -m1 "TOO_DEEP_RECURSION"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"
