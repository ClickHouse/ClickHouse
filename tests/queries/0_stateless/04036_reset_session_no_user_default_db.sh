#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A user with NO `DEFAULT DATABASE`, reached over a transport that doesn't
# capture a connection-start database, must fall through to the third
# candidate in `Context::resetToUserDefaults`: the global context's current
# database (whatever a fresh authentication would land on, typically
# `default`). HTTP is the natural test transport here because the native TCP
# handler does call `rememberDatabaseAtSessionStart` and would short-circuit
# this branch — which is exactly what reviewer feedback flagged on the
# prior TCP-based shape of this test.

USER="reset_no_default_user_${CLICKHOUSE_DATABASE}"
SID="reset_no_default_session_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER};"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE USER ${USER};
    GRANT ALL ON *.* TO ${USER};
"

# Capture the server's global default database (the third candidate in the
# reset fallback chain). Pull it from `system.server_settings` rather than
# reading `currentDatabase()` over a TCP session, which would echo the
# `--database` flag baked into `${CLICKHOUSE_CLIENT}` and not the global
# default. This keeps the post-reset assertion correct on servers configured
# with a non-`default` global default database.
GLOBAL_DEFAULT_DB=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.server_settings WHERE name = 'default_database'")

# Build the HTTP base URL without the baked-in `database=${CLICKHOUSE_DATABASE}`
# parameter. We deliberately avoid `${CLICKHOUSE_URL}` because that param
# would set `current_database` on every request's query context, hiding the
# post-reset value we want to read back.
HTTP_BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

http() {
    ${CLICKHOUSE_CURL} -sS \
        -H "X-ClickHouse-User: ${USER}" \
        "${HTTP_BASE}?session_id=${SID}&session_timeout=60" \
        --data-binary "$1"
}

# Request 1: dirty the session by `USE`-ing somewhere other than the global
# default. `USE` propagates to the session context.
http "USE system"
http "SELECT 'pre-reset session db:', currentDatabase()"

# Request 2: reset.
http "RESET SESSION"

# Request 3: read back `currentDatabase()` AFTER reset. Each HTTP request
# creates a fresh query context as a copy of the session context, so the
# session-context state set by `RESET SESSION` is what we observe here.
# Asserting in the SAME request as `RESET SESSION` would be wrong: the query
# context was copied from the pre-reset session and `resetToUserDefaults`
# only touches the session context, not the in-flight query context.
http "SELECT 'after reset non-empty:', currentDatabase() != ''"
http "SELECT 'after reset matches global default:', currentDatabase() = '${GLOBAL_DEFAULT_DB}'"
http "SELECT 'after reset is NOT system:', currentDatabase() != 'system'"
