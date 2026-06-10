#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `RESET SESSION` falls back from the dropped connection-start database to the
# user's profile default rather than landing on an empty current_database.
# (The 04028 case where the dropped database is one we `USE`-d, not the one
# we opened the connection with, never exercises this fallback because the
# original connection-start database is still around.)

USER="reset_start_db_user_${CLICKHOUSE_DATABASE}"
DB_A="reset_start_db_a_${CLICKHOUSE_DATABASE}"
DB_B="reset_start_db_b_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -m -q "
        DROP USER IF EXISTS ${USER};
        DROP DATABASE IF EXISTS ${DB_A};
        DROP DATABASE IF EXISTS ${DB_B};
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE DATABASE ${DB_A};
    CREATE DATABASE ${DB_B};
    CREATE USER ${USER} DEFAULT DATABASE ${DB_B};
    GRANT ALL ON *.* TO ${USER};
"

# Open the session with --database=DB_A so the TCP handshake captures DB_A as
# database_at_session_start. `${CLICKHOUSE_CLIENT}` already includes
# `--database=${CLICKHOUSE_DATABASE}`; rewrite it to point at DB_A so we don't
# end up with two `--database` flags (which the client rejects). See
# 01160_table_dependencies.sh for the same pattern.
CLICKHOUSE_CLIENT_DB_A=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--database=${CLICKHOUSE_DATABASE}/--database=${DB_A}/g")

${CLICKHOUSE_CLIENT_DB_A} --user "${USER}" -m -q "
    SELECT 'connection-start db is A:', currentDatabase() = '${DB_A}';
    USE system;
    DROP DATABASE ${DB_A};
    RESET SESSION;
    SELECT 'after drop + reset, fall back to user default B:', currentDatabase() = '${DB_B}';
" | sed "s/_${CLICKHOUSE_DATABASE}/_DB/g"
