#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t0"

rm -rf "${ICEBERG_TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t0 (id Int32, name String)
    ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}', 'Parquet');
"

echo '--- system.iceberg_metadata_log_entries schema ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT name, type
    FROM system.columns
    WHERE database = 'system' AND table = 'iceberg_metadata_log_entries'
    ORDER BY position
    FORMAT TSV;
"

echo '--- empty table ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        table,
        endsWith(file, '.metadata.json')  AS file_ends_with_metadata_json,
        latest_snapshot_id IS NULL        AS latest_snapshot_id_is_null,
        latest_schema_id,
        latest_sequence_number
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY timestamp, file
    FORMAT Vertical;
"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    INSERT INTO t0 VALUES (1, 'a');
    INSERT INTO t0 VALUES (2, 'b');
"

echo '--- after two inserts ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        table,
        endsWith(file, '.metadata.json')  AS file_ends_with_metadata_json,
        latest_snapshot_id IS NULL        AS latest_snapshot_id_is_null,
        latest_schema_id,
        latest_sequence_number
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY timestamp, file
    FORMAT Vertical;
"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    ALTER TABLE t0 DELETE WHERE id = 1;
"

echo '--- after the delete ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        table,
        latest_snapshot_id IS NULL        AS latest_snapshot_id_is_null,
        latest_schema_id,
        latest_sequence_number
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY timestamp, file
    FORMAT Vertical;
"

echo '--- latest snapshots are the snapshots of system.iceberg_history ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count(),
        countIf(latest_snapshot_id IN (
            SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't0'))
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = 't0' AND latest_snapshot_id IS NOT NULL;
"

rm -rf "${ICEBERG_TABLE_PATH}"
