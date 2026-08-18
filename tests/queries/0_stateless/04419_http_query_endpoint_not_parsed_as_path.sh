#!/usr/bin/env bash
# The legacy query endpoints `/` and `/query` must keep working as the query API even when the user
# has the path-as-file features enabled. They must NOT be parsed as table-as-file paths: otherwise
# `/query?query=SELECT+1` reads `query` as a table name and throws `UNKNOWN_TABLE default.query`
# before the SQL runs. Regression test for that bypass (the factory claims these legacy routes, but
# `processQuery` used to still parse the path for them).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
USER="user_${CLICKHOUSE_DATABASE}"

# Enable all path features per-user so the test does not depend on the global test profile.
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS http_allow_database_as_path = 1, http_allow_table_as_file = 1, http_allow_filters_as_path = 1"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO ${USER}"

AUTH="user=${USER}"

echo "-- /query?query=... is the query endpoint, not a table named 'query'"
curl -sS "${BASE_URL}/query?${AUTH}&query=SELECT%201"

echo "-- /?query=... is the root query endpoint"
curl -sS "${BASE_URL}/?${AUTH}&query=SELECT%202"

echo "-- POST body to the root query endpoint still works"
echo 'SELECT 3' | curl -sS "${BASE_URL}/?${AUTH}" --data-binary @-

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
