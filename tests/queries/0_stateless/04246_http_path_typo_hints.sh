#!/usr/bin/env bash

# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"

# A table whose name is close to the built-in `/dashboard` HTTP handler, so the URL
# `/dashbord` is close to both a real table and a real handler.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.dashboards"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.dashboards (x Int) ENGINE=Memory"

# Grep for each hint substring independently so the output stays free of raw
# `DB::Exception` text (which the test framework flags as a failure).

echo "===== /dashbord  (close to both /dashboard handler and 'dashboards' table) ====="
out=$(${CLICKHOUSE_CURL} -sS -H "X-ClickHouse-Database: ${DB}" "${BASE_URL}/dashbord")
echo "$out" | grep -oE "There is no handle /dashbord\."
echo "$out" | grep -oE "Maybe you meant /dashboard\?"
echo "$out" | grep -oE "maybe you meant table .?dashboards.?"

echo "===== /sashboards  (no close table name; only handler hint applies) ====="
out=$(${CLICKHOUSE_CURL} -sS -H "X-ClickHouse-Database: ${DB}" "${BASE_URL}/sashboards")
echo "$out" | grep -oE "There is no handle /sashboards\."
echo "$out" | grep -oE "Maybe you meant /dashboard\?"

echo "===== /dashbord_db/hits  (missing database; handler hint /dashboard applies) ====="
out=$(${CLICKHOUSE_CURL} -sS "${BASE_URL}/dashbord_db/hits")
echo "$out" | grep -oE "Database dashbord_db does not exist"
echo "$out" | grep -oE "Or maybe HTTP handler /dashboard\?"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DB}.dashboards"
