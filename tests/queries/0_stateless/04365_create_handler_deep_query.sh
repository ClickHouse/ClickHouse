#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the handler's query parameters must be collected from the already-parsed AST, not by
# re-parsing the formatted query string with the default parser limits. A query that is valid under the
# session's raised `max_parser_depth` / `max_parser_backtracks` would otherwise be rejected at creation
# time, because the re-parse used `DBMS_DEFAULT_MAX_PARSER_DEPTH` / `DBMS_DEFAULT_MAX_PARSER_BACKTRACKS`.

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_deep_${CLICKHOUSE_DATABASE}"
URL="/deep_${CLICKHOUSE_DATABASE}"

# An expression nested deeper than the default parser depth (1000), plus a query parameter.
DEEP="$(python3 -c "print('[' * 1100 + '1' + ']' * 1100)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"

# Created with raised parser limits; the AS query references a parameter. With the fix this succeeds; the
# old code threw TOO_DEEP_RECURSION while re-parsing the formatted query with the default depth limit.
${CLICKHOUSE_CLIENT} --max_parser_depth=5000 --max_parser_backtracks=50000000 --query "
    CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT ${DEEP}[1] AS a, {x:UInt64} AS p
" && echo "created"

# A plain DROP (no IF EXISTS) succeeds only if the handler was actually created; this also avoids reading
# system.handlers, which would re-parse the deep statement at the session parser limits.
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}" && echo "dropped"
