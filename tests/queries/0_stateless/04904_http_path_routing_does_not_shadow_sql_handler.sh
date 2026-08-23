#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `http_allow_path_requests` on, the dynamic query handler claims *any* request path so it can
# interpret it as `/database/table.format`. It therefore has to be the last handler consulted: it does
# not own those paths. In particular a SQL-defined handler (`CREATE HANDLER`), which is registered
# after the configured and built-in handlers, must still win over it - otherwise every SQL-defined
# handler becomes unreachable as soon as path requests are enabled.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/hshadow_${DB}"
H="hshadow_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`;"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$H\` URL '${P}/get' AS SELECT 'from the handler' AS r FORMAT TSV"

echo "=== GET reaches the SQL-defined handler, not the path-as-file router ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/get"

echo "=== so does HEAD (it reuses the handler declared for GET) ==="
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' -I "${BASE}${P}/get"

echo "=== the OPTIONS preflight is answered for the handler's own path ==="
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' -X OPTIONS "${BASE}${P}/get"

echo "=== a path the handler does not match is still routed to the query handler ==="
# `http_allow_database_as_path` is on for stateless tests, so an unknown database surfaces as a query
# error from the dynamic handler rather than as a plain 404 from the not-found handler.
${CLICKHOUSE_CURL} -sS "${BASE}${P}/nosuchroute" | grep -oE '\(UNKNOWN_DATABASE\)' | head -1
