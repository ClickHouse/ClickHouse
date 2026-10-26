#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler whose AS query is nested deeper than the default parser depth must stay reloadable. The stored
# CREATE HANDLER statement is the server's own validated output, so the paths that re-parse it
# (system.handlers, ALTER HANDLER, restart/background reload) re-parse with unlimited parser limits rather
# than the reading session's `max_parser_depth` / `max_parser_backtracks`. Otherwise a handler created
# under raised limits would be stored successfully and then fail to read or alter at the default limits.

# Per-test-unique name/URL so parallel runs do not interfere (handlers are a global namespace).
HANDLER="h_04417_${CLICKHOUSE_DATABASE}"

# An expression nested deeper than the default parser depth (1000).
DEEP="$(python3 -c "print('[' * 1100 + '1' + ']' * 1100)")"

${CLICKHOUSE_CLIENT} -q "DROP HANDLER IF EXISTS ${HANDLER}"

# Created with raised parser limits; the stored statement nests deeper than the default limit.
${CLICKHOUSE_CLIENT} --max_parser_depth=5000 --max_parser_backtracks=50000000 -q "
    CREATE HANDLER ${HANDLER} URL '/${HANDLER}' AS SELECT ${DEEP}[1] AS a" && echo "created"

# system.handlers re-parses the stored statement for every row (to mask secrets); the default session
# limits previously rejected the deep statement with TOO_DEEP_RECURSION.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.handlers WHERE name = '${HANDLER}'"

# ALTER re-parses the stored statement to merge the change; also previously rejected at default limits.
${CLICKHOUSE_CLIENT} -q "ALTER HANDLER ${HANDLER} METHODS (GET, POST)" && echo "altered"
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(methods) FROM system.handlers WHERE name = '${HANDLER}'"

${CLICKHOUSE_CLIENT} -q "DROP HANDLER ${HANDLER}" && echo "dropped"
