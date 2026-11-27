#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `database` setting is applied by `executeQuery` as the documented equivalent of `USE`. When the
# current database is chosen out-of-band — the user's `DEFAULT DATABASE`, a protocol's connect-time
# database, MySQL `COM_INIT_DB` — `setCurrentDatabase` must mirror the choice back into the setting.
# Regression test: without the mirror, a stale `database` value inherited from the user's profile
# settings would win back in `executeQuery` and silently override the explicitly selected database.

DB1="${CLICKHOUSE_DATABASE}_dbsync1"
DB2="${CLICKHOUSE_DATABASE}_dbsync2"
TEST_USER="user_04814_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB1}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB2}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH no_password SETTINGS database = '${DB1}'"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW DATABASES ON *.* TO ${TEST_USER}"

echo "-- the profile database setting applies when the connection does not choose a database"
${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${TEST_USER}" --query "SELECT currentDatabase() == '${DB1}'"

echo "-- the user's DEFAULT DATABASE wins over the profile database setting"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${TEST_USER} DEFAULT DATABASE ${DB2}"
${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${TEST_USER}" --query "SELECT currentDatabase() == '${DB2}'"

echo "-- USE wins over the profile database setting for subsequent queries in the session"
${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${TEST_USER}" --query "USE ${DB1}; SELECT currentDatabase() == '${DB1}'"

${CLICKHOUSE_CLIENT} --query "DROP USER ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB1}"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB2}"
