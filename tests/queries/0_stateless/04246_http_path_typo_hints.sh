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

# Strip release-dependent and run-dependent fragments before diff so the output is stable:
#  - the trailing "(version X.Y.Z)" suffix
#  - the per-run database name (replaced with the literal "<db>")
strip_dynamic() {
    sed -E "s/ \(version [^)]+\)//; s/\\b${DB}\\b/<db>/g"
}

echo "===== /dashbord  (looked up as table; close to both table and handler) ====="
curl -sS -H "X-ClickHouse-Database: ${DB}" "${BASE_URL}/dashbord" | strip_dynamic
echo

echo "===== /sashboards  (no close table name; only handler hint applies) ====="
curl -sS -H "X-ClickHouse-Database: ${DB}" "${BASE_URL}/sashboards" | strip_dynamic
echo

echo "===== /dashbord_db/hits  (missing database; handler hint /dashboard applies) ====="
curl -sS "${BASE_URL}/dashbord_db/hits" | strip_dynamic

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DB}.dashboards"
