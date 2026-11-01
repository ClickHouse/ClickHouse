#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated; REFRESH MV + replicated database + non-replicated target is rejected.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/93278
# SELECT from a materialized view backed by IcebergLocal engine used to crash
# with "Logical error: Can't extract iceberg table state from storage snapshot".
# The root cause: StorageMaterializedView::read() delegates to the target table
# without calling updateExternalDynamicMetadataIfExists(), so the storage
# snapshot lacks datalake_table_state when IcebergMetadata::iterate() is called.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${TABLE}_mv"

# Create a materialized view with IcebergLocal target.
${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW ${TABLE}_mv
    REFRESH AFTER 1 MINUTE
    ENGINE = IcebergLocal('${TABLE_PATH}')
    AS (SELECT 1::Int32 AS c0)
"

# This used to crash the server with LOGICAL_ERROR.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE}_mv"

# Insert data and verify it's readable through the view.
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE}_mv VALUES (42)" --allow_insert_into_iceberg 1
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE}_mv"

# Clean up.
${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${TABLE}_mv"
rm -rf "${TABLE_PATH}" 2>/dev/null

echo "OK"
