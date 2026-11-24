#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler must stay invokable in a session whose parser limits are lower than the nesting of the stored
# query text: the text is the server's own validated output, so the invocation path parses it with unlimited
# depth and backtracks - the same way every reload does - instead of the caller's `max_parser_depth` /
# `max_parser_backtracks`. Regression coverage for the actual HTTP invocation (`04365_create_handler_deep_query`
# only covers creation).

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name, URL and session so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_deepinv_${CLICKHOUSE_DATABASE}"
URL="/deepinv_${CLICKHOUSE_DATABASE}"
SESSION="deepinv_${CLICKHOUSE_DATABASE}"

# Nested deeper than the parser depth the invoking session is limited to below. The nesting is kept modest
# on purpose: a query deep enough to exceed the default limit of 1000 exhausts the thread stack of a
# sanitizer build before the depth limit is ever reached, which tests nothing about handlers.
DEEP="$(python3 -c "print('[' * 30 + '1' + ']' * 30)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"

echo "=== created ==="
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT length(${DEEP}[1]) AS len, {x:UInt64} AS p
" && echo "created"

# A session that cannot parse the handler's query text itself.
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "SET max_parser_depth = 10"

echo "=== the session parser limit is in effect ==="
${CLICKHOUSE_CURL} -sS "${BASE}/?session_id=${SESSION}" -d "SELECT length(${DEEP}[1])" 2>&1 | grep -o -m1 "TOO_DEEP_RECURSION"

echo "=== invoked over HTTP in that session ==="
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?session_id=${SESSION}&param_x=5"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"
