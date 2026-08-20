#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Selecting the current (default) database has never required the `SHOW_DATABASES` privilege:
# the native protocol applies the client's default database with a bare `setCurrentDatabase`,
# and the HTTP `database` URL parameter historically behaved the same. The `database` setting
# must keep this contract on every carrier (in-query `SETTINGS`, the HTTP `database` URL
# parameter, `X-ClickHouse-Database`), because a user granted access to a single table only
# (e.g. `GRANT SELECT ON db.table`) must be able to name that database as the default.

TEST_USER="user_04619_${CLICKHOUSE_DATABASE}"
BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE granted_tb (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE secret_tb (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --query "INSERT INTO granted_tb VALUES (1)"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.granted_tb TO ${TEST_USER}"

echo "-- HTTP database URL parameter works without SHOW_DATABASES"
${CLICKHOUSE_CURL} -sS "${BASE_URL}?user=${TEST_USER}&database=${CLICKHOUSE_DATABASE}&query=SELECT+n+FROM+granted_tb"

echo "-- database setting in query works without SHOW_DATABASES"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT n FROM granted_tb SETTINGS database = '${CLICKHOUSE_DATABASE}'"

echo "-- native client default database works without SHOW_DATABASES"
${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${TEST_USER}" --database "${CLICKHOUSE_DATABASE}" --query "SELECT n FROM granted_tb"

echo "-- table privileges are still enforced"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT n FROM secret_tb SETTINGS database = '${CLICKHOUSE_DATABASE}'" 2>&1 | grep -o "ACCESS_DENIED" | head -1
${CLICKHOUSE_CURL} -sS "${BASE_URL}?user=${TEST_USER}&database=${CLICKHOUSE_DATABASE}&query=SELECT+n+FROM+secret_tb" 2>&1 | grep -o "ACCESS_DENIED" | head -1

${CLICKHOUSE_CLIENT} --query "DROP USER ${TEST_USER}"
