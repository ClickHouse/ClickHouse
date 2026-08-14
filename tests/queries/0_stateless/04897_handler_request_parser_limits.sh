#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler must stay invokable when the *request* lowers the parse limits. The invocation path parses the
# server's own stored query text with unlimited depth and backtracks and with a size limit that fits the text,
# and a request names settings freely - so `?max_parser_depth=...`, `?max_parser_backtracks=...` and
# `?max_query_size=...` must not lower those limits again for the handler's query.
# `04844_handler_deep_query_invocation` covers the same contract for a session-level limit.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_parserlimits_${CLICKHOUSE_DATABASE}"
URL="/parserlimits_${CLICKHOUSE_DATABASE}"

# Nested deeper than the parser depth the requests below are limited to. The nesting is kept modest on
# purpose: a query deep enough to exceed the default limit of 1000 exhausts the thread stack of a sanitizer
# build before the depth limit is ever reached, which tests nothing about handlers.
DEEP="$(python3 -c "print('[' * 30 + '1' + ']' * 30)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"

echo "=== created ==="
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT length(${DEEP}[1]) AS len, {x:UInt64} AS p
" && echo "created"

echo "=== the request parse limits are in effect for an ordinary query ==="
${CLICKHOUSE_CURL} -sS "${BASE}/?max_parser_depth=10" -d "SELECT length(${DEEP}[1])" 2>&1 | grep -o -m1 "TOO_DEEP_RECURSION"
${CLICKHOUSE_CURL} -sS "${BASE}/?max_parser_backtracks=2" -d "SELECT length(${DEEP}[1])" 2>&1 | grep -o -m1 "TOO_SLOW_PARSING"
${CLICKHOUSE_CURL} -sS "${BASE}/?max_query_size=10" -d "SELECT length(${DEEP}[1])" 2>&1 | grep -o -m1 "Max query size exceeded"

echo "=== invoked over HTTP with the same request settings ==="
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?max_parser_depth=10&param_x=5"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?max_parser_backtracks=2&param_x=6"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?max_query_size=10&param_x=7"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"
