-- Tags: no-fasttest

-- A table written by Spark: metadata-log lists only the previous metadata files.
DROP TABLE IF EXISTS spark_t;
CREATE TABLE spark_t
ENGINE = IcebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

SELECT '--- one row per metadata file ---';
SELECT
    basename(file) AS file,
    toUnixTimestamp64Milli(timestamp) AS timestamp_ms,
    latest_snapshot_id IS NULL AS latest_snapshot_id_is_null,
    latest_schema_id,
    latest_sequence_number
FROM system.iceberg_metadata_log_entries
WHERE database = currentDatabase() AND table = 'spark_t'
ORDER BY timestamp;

SELECT '--- latest snapshots are the snapshots of system.iceberg_history ---';
SELECT
    count(),
    countIf(latest_snapshot_id IN (
        SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 'spark_t'))
FROM system.iceberg_metadata_log_entries
WHERE database = currentDatabase() AND table = 'spark_t' AND latest_snapshot_id IS NOT NULL;
