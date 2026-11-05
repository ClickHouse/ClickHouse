#!/usr/bin/env bash

# Tags: no-replicated-database
# ^ explicit database names + a per-user default `database` setting; ReplicatedDatabase rewrites
#   database names, which breaks the fixed `db1`/`db2` resolution this test asserts on.

# Regression test: a database supplied in the HTTP URL path must win over an inherited *profile
# default* for the `database` setting. Previously `HTTPHandler` set the current database from the
# path via `setCurrentDatabase` but left the `database` setting unchanged, so `executeQuery` later
# re-applied the stale profile default (e.g. `database = 'db1'`) and switched the query back —
# making unqualified names resolve in the wrong database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# `db2` is the path-supplied database (the per-test database); `db1` is what the user's profile
# default `database` setting points at. A table named `marker` exists in both, with distinct
# contents, so unqualified resolution reveals which database is current.
DB2="${CLICKHOUSE_DATABASE}"
DB1="${CLICKHOUSE_DATABASE}_default"
USER="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB1}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB1}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB1}.marker (s String) ENGINE=Memory AS SELECT 'from_db1'"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB2}.marker (s String) ENGINE=Memory AS SELECT 'from_db2'"

# A user whose profile default for the `database` setting is db1 (NOT supplied per request).
# Path features are also enabled per-user so the test does not depend on the global test profile.
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS database='${DB1}', http_allow_database_as_path=1, http_allow_table_as_file=1"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB1}.* TO ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB2}.* TO ${USER}"

AUTH="user=${USER}"

echo "-- sanity: WITHOUT a path the user falls back to its profile default db1"
curl -sS "${BASE_URL}/?${AUTH}&query=SELECT%20s%20FROM%20marker"

echo "-- /db2/marker: unqualified 'marker' must resolve in the path database db2, not db1"
curl -sS "${BASE_URL}/${DB2}/marker?${AUTH}&query=SELECT%20s%20FROM%20marker"

echo "-- /db2 (table-as-file off): currentDatabase() must be db2, not db1"
RESULT=$(curl -sS "${BASE_URL}/${DB2}?${AUTH}&http_allow_table_as_file=0&query=SELECT%20currentDatabase()")
if [ "${RESULT}" = "${DB2}" ]; then echo "currentDatabase OK (db2)"; else echo "currentDatabase WRONG: ${RESULT}"; fi

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB1}"
