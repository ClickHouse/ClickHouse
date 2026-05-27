#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A user with NO `DEFAULT DATABASE` shouldn't land on an empty `currentDatabase`
# after `RESET SESSION`. `Context::setUser` leaves the database inherited from
# the global context in that case (typically `default`), so reset must match
# that behaviour rather than clearing.

USER="reset_no_default_user_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER};"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE USER ${USER};
    GRANT ALL ON *.* TO ${USER};
"

# Capture the start-up database from a separate invocation (the user has no
# DEFAULT DATABASE, so this is whatever the global context's current database
# is — typically 'default').
START_DB=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT currentDatabase()")

# Single TCP session: USE away, RESET, assert we're back on START_DB.
${CLICKHOUSE_CLIENT} --user "${USER}" -m -q "
    SELECT 'start db non-empty:', currentDatabase() != '';
    USE system;
    SELECT 'after USE:', currentDatabase();
    RESET SESSION;
    SELECT 'after reset non-empty:', currentDatabase() != '';
    SELECT 'after reset matches start:', currentDatabase() = '${START_DB}';
"
