#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `database` setting is documented as the equivalent of `USE`, so every carrier of it
# (in-query `SETTINGS`, the HTTP `database` URL parameter, the `/database/table` URL path)
# must enforce the same `SHOW_DATABASES` privilege as `USE` — and must not serve as a
# database-existence oracle for unprivileged users.

TEST_USER="user_04619_${CLICKHOUSE_DATABASE}"
SECRET_DB="db_04619_secret_${CLICKHOUSE_DATABASE}"
BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${SECRET_DB}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH no_password DEFAULT DATABASE ${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, SHOW ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER}"

echo "-- database setting in query: denied"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT currentDatabase() SETTINGS database = '${SECRET_DB}'" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "-- database setting for a nonexistent database: same error, no existence oracle"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT currentDatabase() SETTINGS database = '${SECRET_DB}_nonexistent'" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "-- HTTP database URL parameter: denied"
${CLICKHOUSE_CURL} -sS "${BASE_URL}?user=${TEST_USER}&database=${SECRET_DB}&query=SELECT+currentDatabase()" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "-- USE keeps behaving the same"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "USE ${SECRET_DB}" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "-- granted database still works"
${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT currentDatabase() == '${CLICKHOUSE_DATABASE}' SETTINGS database = '${CLICKHOUSE_DATABASE}'"

${CLICKHOUSE_CLIENT} --query "DROP USER ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${SECRET_DB}"
