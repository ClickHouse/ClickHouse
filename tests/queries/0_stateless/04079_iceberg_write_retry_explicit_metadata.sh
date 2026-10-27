#!/usr/bin/env bash
# Tags: no-fasttest

# A write to a table pinned by iceberg_metadata_file_path (for time-travel reads)
# must land on the newest committed version, not on the pinned one.
#
# This began as a regression test for the retry loop: the first attempt used to
# resolve through the pin, target a version that already exists, and rely on the
# retry to discover the real latest. The write root now resolves the latest
# directly, so the conflict no longer arises and the retry loop is not reached.
# What is asserted here is the outcome, and it holds either way.

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

# Step 3: INSERT while pinned to v1.
# The write ignores the pin, resolves v3 and creates v4. Pinning at the version
# the write starts from is what once made this fail: targeting v2, which Step 1
# already wrote, and burning all 100 retries into DATALAKE_DATABASE_ERROR.
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
