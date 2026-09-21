#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `system.iceberg_history` requires the USE_AVRO build option.

# A DataLakeCatalog database whose catalog cannot be built makes the table listing fail. That failure is
# deliberately tolerated so that a query enumerating databases still succeeds, so it must not be logged at
# error level: `send_logs_level` forwards error entries to the client, which reports them as errors of a
# query that succeeded, and clickhouse-test fails any test whose stderr is non-empty.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_unreachable_catalog"
QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_listing"
STDERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null
    rm -f "${STDERR_FILE}" 2>/dev/null
}
trap cleanup EXIT

# `exact_header` is forbidden by tests/config/config.d/forbidden_headers.xml. ATTACH does not validate the
# header, so the rejection happens when the catalog is first built, inside the table listing.
${CLICKHOUSE_CLIENT} --query "
    ATTACH DATABASE ${DB} ENGINE = DataLakeCatalog('http://localhost:18181/v1')
    SETTINGS catalog_type = 'rest', auth_header = 'exact_header: some_value', warehouse = 'demo'
"

${CLICKHOUSE_CLIENT} --send_logs_level=warning --show_data_lake_catalogs_in_system_tables=0 \
    --query_id "${QUERY_ID}" \
    --query "SELECT count() FROM system.iceberg_history WHERE database = '${DB}'" 2> "${STDERR_FILE}"

if [ -s "${STDERR_FILE}" ]; then echo "client got logs: 1"; else echo "client got logs: 0"; fi

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
echo "site level: $(${CLICKHOUSE_CLIENT} --query "
    SELECT level FROM system.text_log
    WHERE query_id = '${QUERY_ID}' AND logger_name = 'DatabaseDataLake(${DB})'
    ORDER BY event_time_microseconds
    LIMIT 1")"
echo "entries above information: $(${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.text_log
    WHERE query_id = '${QUERY_ID}' AND level IN ('Fatal', 'Critical', 'Error', 'Warning')")"
