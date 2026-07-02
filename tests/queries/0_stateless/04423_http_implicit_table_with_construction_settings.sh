#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: the explicit FROM-less path relies on `implicit_table_at_top_level`, which is
# honored only by the analyzer (`QueryTreeBuilder`/`QueryAnalyzer`); the old interpreter resolves a
# FROM-less `SELECT` against the dummy `system.one`. The feature is new-analyzer-only.

# Regression: a FROM-less HTTP path query (`/db/t?query=SELECT x`) combined with a query-construction
# setting (`limit` / `filter` / `order` / ...) must still resolve unqualified columns against the path
# table. The construction settings wrap the base query as a derived table
# (`SELECT * FROM (SELECT x) LIMIT n`); the single-arm base then becomes a subquery, where the
# analyzer does NOT apply `implicit_table_at_top_level`, so before the fix `SELECT x` fell back to
# `system.one` and `x` failed to resolve. The implicit path table is now materialized as an explicit
# `FROM` on the FROM-less base before wrapping, so it keeps reading the path table once nested.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
USER="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.t (x UInt64) ENGINE = Memory AS SELECT number + 1 FROM numbers(5)"

# Enable the path features per-user so the test does not depend on the global test profile (the
# server-level `http_allow_path_requests` flag is provided by the test config).
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS http_allow_database_as_path = 1, http_allow_table_as_file = 1"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${DB}.* TO ${USER}"

AUTH="user=${USER}"

echo "-- control: FROM-less aggregate, no construction setting (resolves the path table)"
curl -sS "${BASE_URL}/${DB}/t?${AUTH}&query=SELECT%20sum(x)"

echo "-- FROM-less SELECT + order + limit (wrapping must keep the path table)"
curl -sS "${BASE_URL}/${DB}/t?${AUTH}&query=SELECT%20x&order=x&limit=2"

echo "-- FROM-less SELECT + filter + order (wrapping must keep the path table)"
curl -sS "${BASE_URL}/${DB}/t?${AUTH}&query=SELECT%20x&filter=x%3E3&order=x"

echo "-- FROM-less SELECT with an alias + order on the alias + limit"
curl -sS "${BASE_URL}/${DB}/t?${AUTH}&query=SELECT%20x%20AS%20y&order=y&limit=1"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DB}.t"
