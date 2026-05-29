#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# When the native handshake sends no database, `RESET SESSION` must re-read the
# user's `DEFAULT DATABASE` from access control rather than pin the value the
# session happened to start with. So an `ALTER USER ... DEFAULT DATABASE` issued
# during the session takes effect on the next reset.
#
# The native TCP handler only captures a connection-start database when one was
# actually supplied at handshake; connecting without `--database` leaves it
# unset, so the reset falls through to the freshly re-derived user default.

USER="reset_alter_db_user_${CLICKHOUSE_DATABASE}"
DB_A="reset_alter_db_a_${CLICKHOUSE_DATABASE}"
DB_B="reset_alter_db_b_${CLICKHOUSE_DATABASE}"

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
    CREATE USER ${USER} DEFAULT DATABASE ${DB_A};
    GRANT SELECT ON *.* TO ${USER};
    GRANT ALTER USER ON *.* TO ${USER};
"

# Strip the baked-in `--database=${CLICKHOUSE_DATABASE}` so the handshake sends
# no database and the server applies the user's `DEFAULT DATABASE` instead.
CLIENT_NO_DB="${CLICKHOUSE_CLIENT/--database=${CLICKHOUSE_DATABASE}/}"

# One TCP session: confirm the start database is the user default, change that
# default mid-session, dirty the database with USE, then reset. The post-reset
# database must be the NEW default (re-read), not the one we started with.
${CLIENT_NO_DB} --user "${USER}" -m -q "
    SELECT 'database at session start:', currentDatabase() = '${DB_A}';
    ALTER USER ${USER} DEFAULT DATABASE ${DB_B};
    USE system;
    SELECT 'database after USE:', currentDatabase();
    RESET SESSION;
    SELECT 'database after RESET picks up new default:', currentDatabase() = '${DB_B}';
    SELECT 'database after RESET is not the start default:', currentDatabase() != '${DB_A}';
"
