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

# `exact_header` is forbidden by tests/config/config.d/forbidden_headers.xml. A new definition is rejected,
# also by a full-definition ATTACH.
${CLICKHOUSE_CLIENT} --query "
    ATTACH DATABASE ${DB} ENGINE = DataLakeCatalog('http://localhost:18181/v1')
    SETTINGS catalog_type = 'rest', auth_header = 'exact_header: some_value', warehouse = 'demo'
" 2>&1 | grep -o -m1 'is forbidden'

# A stored definition is not validated again, so a catalog that cannot be built fails inside the table
# listing. The database is stored in a clickhouse-local data directory with a malformed `auth_header`,
# which the catalog rejects when it is first built; the start loads it like server startup does.
rm -rf "${CLICKHOUSE_TMP:?}/05240_data"
mkdir -p "${CLICKHOUSE_TMP:?}/05240_data/metadata"
cat > "${CLICKHOUSE_TMP:?}/05240_data/metadata/baddb.sql" <<'EOF'
ATTACH DATABASE baddb
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', auth_header = 'malformed_without_colon', warehouse = 'demo'
EOF

# `system.iceberg_history` reaches the lightweight iterator, while `system.iceberg_files` reaches the general
# one (`getTablesIterator`), which is also what `SELECT ... FROM system.tables` uses when it needs storage objects.
for table in iceberg_history iceberg_files; do
    ${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP:?}/05240_data" --send_logs_level=warning --show_data_lake_catalogs_in_system_tables=0 \
        --query "SELECT count() FROM system.${table} WHERE database = 'baddb'" 2> "${CLICKHOUSE_TMP:?}/05240_listing.stderr"
    if [ -s "${CLICKHOUSE_TMP:?}/05240_listing.stderr" ]; then echo "client got logs (${table}): 1"; else echo "client got logs (${table}): 0"; fi

    # The listing failure is still logged, below error level.
    ${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP:?}/05240_data" --send_logs_level=trace --show_data_lake_catalogs_in_system_tables=0 \
        --query "SELECT count() FROM system.${table} WHERE database = 'baddb' FORMAT Null" 2> "${CLICKHOUSE_TMP:?}/05240_listing.stderr"
    echo "site level (${table}): $(grep -o -m1 '<[A-Za-z]*> DatabaseDataLake(baddb): Cannot list the tables' "${CLICKHOUSE_TMP:?}/05240_listing.stderr" | grep -o '^<[A-Za-z]*>')"
    echo "entries above information (${table}): $(grep -c -E '<(Fatal|Critical|Error|Warning)>' "${CLICKHOUSE_TMP:?}/05240_listing.stderr")"
done
rm -rf "${CLICKHOUSE_TMP:?}/05240_data" "${CLICKHOUSE_TMP:?}/05240_listing.stderr"
