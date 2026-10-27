#!/usr/bin/env bash

# A table whose name contains a comparison character (`=`, `>`, `<`, `!`) must be addressable via the
# HTTP URL path when back-quoted (`/db/%60a=1%60`), even with `http_allow_filters_as_path` enabled.
# Without the back-quote escape hatch running before filter parsing, `parseHTTPPath` misreads the
# component as a `WHERE a = 1` filter and targets the database itself as if it were the table, so the
# request resolves the wrong table (or fails) instead of reading the named table.
#
# An unquoted component with a comparison character must still be interpreted as a filter, so the fix
# must be limited to fully back-quoted components.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
USER="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.\`a=1\` (x UInt64) ENGINE = Memory AS SELECT 111"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.\`b>2\` (x UInt64) ENGINE = Memory AS SELECT 222"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.flt (a UInt64) ENGINE = Memory AS SELECT arrayJoin([1, 2, 3])"

# Enable the path features per-user so the test does not depend on the global test profile (the
# server-level `http_allow_path_requests` flag is provided by the test config).
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS http_allow_database_as_path = 1, http_allow_table_as_file = 1, http_allow_filters_as_path = 1"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB}.* TO ${USER}"

AUTH="user=${USER}"

echo "-- back-quoted table name containing '=' resolves to the table, not a filter"
curl -sS "${BASE_URL}/${DB}/%60a=1%60?${AUTH}"

echo "-- back-quoted table name containing '>' resolves to the table, not a filter"
curl -sS "${BASE_URL}/${DB}/%60b%3E2%60?${AUTH}"

echo "-- an unquoted component with a comparison character still applies as a WHERE filter"
curl -sS "${BASE_URL}/${DB}/a=3/flt?${AUTH}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
