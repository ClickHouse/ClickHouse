#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# Some PostgreSQL drivers (e.g. Skunk) send session-management commands such as
# `RESET ALL` and `UNLISTEN *` during connection setup or cleanup. ClickHouse has
# no equivalent for these, but it must accept them as no-ops instead of failing
# with a syntax error, so that such drivers can connect over the wire protocol.
# See https://github.com/ClickHouse/ClickHouse/issues/12476

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times
# concurrently, and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04512_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align 2>&1 <<'EOF'
RESET ALL;
RESET search_path;
RESET some_extension.some_setting;
UNLISTEN *;
UNLISTEN some_channel;
DISCARD ALL;
DISCARD PLANS;
DISCARD SEQUENCES;
DISCARD TEMP;
DISCARD TEMPORARY;
SELECT 1 AS connection_is_still_usable;
EOF

# `LISTEN` / `NOTIFY` are application-visible PostgreSQL pub/sub operations, not connection cleanup,
# and this handler never delivers a `NotificationResponse`. Unlike `UNLISTEN` (idempotent
# unsubscribe-all cleanup), they must not be acknowledged as a silent no-op — even in their
# well-formed single-channel form — so a client relying on pub/sub gets a plain error instead of a
# false success. Issue https://github.com/ClickHouse/ClickHouse/issues/12476 only asked for
# `UNLISTEN *` / `RESET ALL`.
for unsupported in "LISTEN some_channel" "NOTIFY some_channel"; do
    if psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align \
            -c "${unsupported}" 2>&1 | grep -qi error; then
        echo "unsupported '${unsupported}' not silently accepted"
    else
        echo "UNEXPECTED: unsupported '${unsupported}' silently accepted"
    fi
done

# A no-op driver command must not silently swallow a trailing statement when both arrive in a
# single simple-query packet (e.g. `RESET ALL; SELECT 1`). Such a packet must fall through to the
# normal multi-statement splitter instead of being acknowledged as a bare `RESET`; here that means
# it surfaces an error rather than silently succeeding.
if psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align \
        -c "RESET ALL; SELECT 1 AS trailing_query" 2>&1 | grep -qi error; then
    echo "multi-statement packet not silently accepted"
else
    echo "UNEXPECTED: multi-statement packet silently accepted"
fi

# The no-op handling covers only the exact PostgreSQL command forms; a malformed variant must not be
# claimed as a successful no-op. It must fall through to the normal path and surface an error instead.
# This includes trailing garbage after a well-formed argument (e.g. `RESET foo bar`) and a keyword
# that does not end at a word boundary (e.g. `RESET1foo`).
for malformed in "DISCARD FOO" "DISCARD" "RESET" \
                 "RESET foo bar" "UNLISTEN * garbage" \
                 "DISCARD ALL extra" "RESET1foo"; do
    if psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align \
            -c "${malformed}" 2>&1 | grep -qi error; then
        echo "malformed '${malformed}' not silently accepted"
    else
        echo "UNEXPECTED: malformed '${malformed}' silently accepted"
    fi
done

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${PG_USER};"
