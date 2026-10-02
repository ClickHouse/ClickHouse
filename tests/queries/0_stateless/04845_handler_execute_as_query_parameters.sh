#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The statement wrapped in `EXECUTE AS <user>` runs in an impersonated copy of the query context, and that copy
# must keep the query parameters: they belong to the query the caller sent, not to the identity it runs under.
# This matters doubly for SQL-defined HTTP handlers, whose parameters are validated at CREATE HANDLER time and
# bound from the request - without the forwarding, an `AS EXECUTE AS u SELECT {x:UInt64}` handler is accepted at
# creation but every invocation fails with UNKNOWN_QUERY_PARAMETER.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL so parallel tests do not interfere (handlers and users are global namespaces).
APP_USER="appuser_04845_${CLICKHOUSE_DATABASE}"
HANDLER="h_execp_${CLICKHOUSE_DATABASE}"
URL="/execp_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${APP_USER}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE USER ${APP_USER}"

echo "=== EXECUTE AS forwards the caller's query parameters ==="
${CLICKHOUSE_CLIENT} --param_x=7 --query "EXECUTE AS ${APP_USER} SELECT {x:UInt64} AS v, currentUser() = '${APP_USER}'"

echo "=== a parameterized EXECUTE AS handler works at invocation, not only at creation ==="
# EXECUTE AS needs the IMPERSONATE privilege, which readonly denies, so the handler must declare a mutating
# method and is invoked over POST.
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER ${HANDLER} URL '${URL}' METHODS (POST) AS EXECUTE AS ${APP_USER} SELECT {x:UInt64} + 1 AS v"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${URL}?param_x=41"
