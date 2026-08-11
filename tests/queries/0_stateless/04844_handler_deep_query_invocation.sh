#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler created from a query that needs raised parser limits must stay invokable under ordinary session
# limits: the stored text is the server's own validated output, so the invocation path parses it with unlimited
# depth and backtracks - the same way every reload does - instead of the caller's `max_parser_depth` /
# `max_parser_backtracks`. Regression coverage for the actual HTTP invocation (`04365_create_handler_deep_query`
# only covers creation).

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_deepinv_${CLICKHOUSE_DATABASE}"
URL="/deepinv_${CLICKHOUSE_DATABASE}"

# An expression nested deeper than the default parser depth (1000), plus a query parameter. The formatted
# handler text keeps the nesting, so re-parsing it at invocation with the default limits would throw
# TOO_DEEP_RECURSION before this fix.
DEEP="$(python3 -c "print('[' * 1100 + '1' + ']' * 1100)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"

echo "=== created with raised parser limits ==="
${CLICKHOUSE_CLIENT} --max_parser_depth=5000 --max_parser_backtracks=50000000 --query "
    CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT length(${DEEP}[1]) AS len, {x:UInt64} AS p
" && echo "created"

echo "=== invoked over HTTP under ordinary session limits ==="
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?param_x=5"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"
