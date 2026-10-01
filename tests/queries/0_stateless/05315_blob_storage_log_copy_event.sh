#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

# Server-side copies (S3 CopyObject and UploadPartCopy) issued by BACKUP are logged as 'Copy' events.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS data;
    CREATE TABLE data (key UInt64, s String) ENGINE = MergeTree ORDER BY key
        SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    INSERT INTO data SELECT number, randomString(1024) FROM numbers(8192);
    OPTIMIZE TABLE data FINAL;
"

prefix="backups/$CLICKHOUSE_DATABASE"

# All files are smaller than the default s3_max_single_operation_copy_size, so they are copied with CopyObject.
$CLICKHOUSE_CLIENT --format Null -q "BACKUP TABLE data TO S3(s3_conn, '$prefix/single') SETTINGS allow_s3_native_copy = 1"
$CLICKHOUSE_CLIENT --format Null -q "BACKUP TABLE data TO S3(s3_conn, '$prefix/no_native') SETTINGS allow_s3_native_copy = 0"
# The ~8 MiB column file exceeds the threshold, so it is copied with UploadPartCopy.
$CLICKHOUSE_CLIENT --format Null --s3_max_single_operation_copy_size 5242880 \
    -q "BACKUP TABLE data TO S3(s3_conn, '$prefix/multipart') SETTINGS allow_s3_native_copy = 1"

$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS blob_storage_log;

    SELECT
        countIf(event_type = 'Copy') > 0,
        countIf(event_type = 'Copy' AND (error_code != 0 OR source_bucket = '' OR source_remote_path = '' OR source_remote_path = remote_path)) = 0,
        countIf(event_type = 'Copy' AND (source_remote_path, data_size) IN (
            SELECT remote_path, size FROM system.remote_data_paths
            WHERE disk_name = 's3_disk'
                AND local_path LIKE '%' || toString((SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'data')) || '%')) > 0,
        countIf(event_type = 'MultiPartUploadCreate') = 0
    FROM system.blob_storage_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND remote_path LIKE '$prefix/single/%';

    SELECT
        countIf(event_type = 'Copy') = 0,
        countIf(event_type IN ('Upload', 'MultiPartUploadWrite')) > 0
    FROM system.blob_storage_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND remote_path LIKE '$prefix/no_native/%';

    SELECT count() > 0 FROM
    (
        SELECT remote_path
        FROM system.blob_storage_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND remote_path LIKE '$prefix/multipart/%'
        GROUP BY remote_path
        HAVING countIf(event_type = 'MultiPartUploadCreate' AND error_code = 0) = 1
            AND countIf(event_type = 'MultiPartUploadComplete' AND error_code = 0) = 1
            AND countIf(event_type = 'Copy' AND error_code = 0 AND data_size > 5242880) > 0
            AND countIf(event_type = 'MultiPartUploadWrite') = 0
    );

    DROP TABLE data;
"
