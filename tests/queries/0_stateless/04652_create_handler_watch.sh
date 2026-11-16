#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CREATE HANDLER ... AS WATCH ...` must be accepted with the default (GET-only) methods:
# `ASTWatchQuery` reports `QueryKind::Create`, but WATCH is a read-only streaming query
# (`InterpreterWatchQuery` checks only `SELECT` access), so it is runnable under the
# `readonly = 2` mode that safe HTTP methods set, and the create-time "mutating query
# needs a mutating method" gate must not fire for it.

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hw_${DB}"
P="/hw_${DB}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${H}"

${CLICKHOUSE_CLIENT} --query "CREATE HANDLER ${H} URL '${P}/watch' AS WATCH ${DB}.lv"
${CLICKHOUSE_CLIENT} --query "SELECT methods FROM system.handlers WHERE name = '${H}'"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${H}"
