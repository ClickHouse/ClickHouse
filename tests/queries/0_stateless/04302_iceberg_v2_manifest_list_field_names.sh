#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test: Iceberg v2 manifest-list avro files must use the field names
# `added_data_files_count`, `existing_data_files_count`, `deleted_data_files_count`
# (not the v1 names `added_files_count`, `existing_files_count`, `deleted_files_count`).
# Other engines (Athena/Trino/Spark) resolve fields by name and will fail with NPE
# if the v1 names are used in a v2 manifest-list.
#
# Also verifies:
# - First snapshot omits parent-snapshot-id (per Iceberg spec)
# - Empty table has empty refs object
# - Unpartitioned table has last-partition-id = 999

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

# Create a v2 Iceberg table (format_version=2 is the default)
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int32, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"

# --- Test 1: Verify initial metadata (empty table) ---
echo "--- Empty table metadata ---"

# last-partition-id should be 999 for unpartitioned tables
FIRST_METADATA=$(find "${TABLE_PATH}/metadata" -maxdepth 1 -name 'v*.metadata.json' | sort | head -1)
cat "${FIRST_METADATA}" | ${CLICKHOUSE_CLIENT} --query "
    SELECT
        JSONExtractInt(line, 'last-partition-id') as last_partition_id
    FROM input('line String') FORMAT LineAsString
"

# refs should be empty object (no keys)
cat "${FIRST_METADATA}" | ${CLICKHOUSE_CLIENT} --query "
    SELECT
        JSONLength(JSONExtractRaw(line, 'refs')) as refs_keys
    FROM input('line String') FORMAT LineAsString
"

# --- Test 2: Insert data and verify manifest-list field names ---
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${TABLE} VALUES (1, 'hello'), (2, 'world')"

echo "--- Manifest-list v2 field names ---"

# The snap-*.avro file is the manifest-list. Read its schema via DESCRIBE.
# v2 spec requires: added_data_files_count, existing_data_files_count, deleted_data_files_count
for snap in $(find "${TABLE_PATH}/metadata" -maxdepth 1 -name 'snap-*.avro' -type f | sort); do
    ${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE file('${snap}', Avro)" \
        | grep -E 'files_count' | awk '{print $1}' | sort
done

# --- Test 3: Verify snapshot metadata after insert ---
echo "--- Snapshot metadata ---"

LATEST_METADATA=$(find "${TABLE_PATH}/metadata" -maxdepth 1 -name 'v*.metadata.json' | sort | tail -1)

# First snapshot should NOT have parent-snapshot-id field
# Use python-style JSON check: look for "parent-snapshot-id" in the snapshot object
grep -c '"parent-snapshot-id"' "${LATEST_METADATA}" | awk '{print "parent-snapshot-id occurrences:", $1}'

# refs.main should now exist and point to a valid snapshot-id (not -1)
cat "${LATEST_METADATA}" | ${CLICKHOUSE_CLIENT} --query "
    SELECT
        JSONExtractInt(JSONExtractRaw(JSONExtractRaw(line, 'refs'), 'main'), 'snapshot-id') > 0 as valid_ref
    FROM input('line String') FORMAT LineAsString
"

# metadata-log should reference the previous metadata file (v1), not the current one (v2)
cat "${LATEST_METADATA}" | ${CLICKHOUSE_CLIENT} --query "
    SELECT
        JSONExtractString(JSONExtractArrayRaw(line, 'metadata-log')[1], 'metadata-file') LIKE '%v1%' as points_to_previous
    FROM input('line String') FORMAT LineAsString
"
