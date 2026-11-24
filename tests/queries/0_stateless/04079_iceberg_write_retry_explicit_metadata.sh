#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for the Iceberg write retry loop bug.
# When a table has iceberg_metadata_file_path set (for time-travel reads),
# the write retry loop must ignore the explicit path and discover the actual
# latest metadata version. Otherwise, the retry keeps targeting the same
# version that already exists, exhausting all 100 retries and failing with
# DATALAKE_DATABASE_ERROR.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

# Step 1: Create table and populate with two INSERTs.
# This produces metadata v1 (CREATE), v2 (INSERT 1), v3 (INSERT 2).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (2)"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"

# Step 2: Re-create the table pointing to v1 metadata (time-travel).
# Without specifying columns, the schema is read from v1.metadata.json.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}
    ENGINE = IcebergLocal('${TABLE_PATH}')
    SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json'
"

# Step 3: INSERT with explicit v1 path.
# The first write attempt reads v1 and targets v2, but v2 already exists
# on disk (from Step 1). This triggers the retry loop.
# BUG (before fix): retry re-reads v1 (explicit path) -> targets v2 again
#   -> conflict -> loop -> DATALAKE_DATABASE_ERROR after 100 retries.
# FIX: retry ignores explicit path -> discovers v3 -> creates v4 -> success.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (3)"

# Step 4: Verify all data is present by reading from the latest metadata.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}
    ENGINE = IcebergLocal('${TABLE_PATH}')
"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

# Clean up.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null

echo "OK"
