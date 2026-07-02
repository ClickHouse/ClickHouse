#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: the explicit FROM-less path relies on `implicit_table_at_top_level`, which is
# honored only by the analyzer (`QueryTreeBuilder`/`QueryAnalyzer`); the old interpreter ignores it
# and resolves a FROM-less `SELECT` against the dummy `system.one`, so `SELECT x` cannot find the
# column there. The feature is new-analyzer-only (the old analyzer is deprecated).

# A table whose name contains a literal dot, addressed via the HTTP URL path (`/db/my.table`), must
# resolve to that table on BOTH paths:
#   - the no-query path, which generates `SELECT * FROM `db`.`my.table``;
#   - the explicit FROM-less path, which carries the table via the `implicit_table_at_top_level`
#     setting. That setting now holds a back-quoted compound identifier which the analyzer parses, so
#     the dot stays part of the table name instead of being split into separate identifier parts
#     (previously `/db/my.table?query=SELECT+*` resolved the wrong identifier `db`.`my`.`table`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
USER="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.\`my.table\` (x UInt64) ENGINE = Memory AS SELECT 42"

# Enable the path features per-user so the test does not depend on the global test profile (the
# server-level `http_allow_path_requests` flag is provided by the test config).
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS http_allow_database_as_path = 1, http_allow_table_as_file = 1"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB}.* TO ${USER}"

AUTH="user=${USER}"

echo "-- no-query path /db/my.table reads the dotted table"
curl -sS "${BASE_URL}/${DB}/my.table?${AUTH}"

echo "-- explicit FROM-less SELECT on /db/my.table resolves the dotted table"
curl -sS "${BASE_URL}/${DB}/my.table?${AUTH}&query=SELECT%20x"

echo "-- explicit FROM-less aggregate on /db/my.table resolves the dotted table"
curl -sS "${BASE_URL}/${DB}/my.table?${AUTH}&query=SELECT%20count()"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
