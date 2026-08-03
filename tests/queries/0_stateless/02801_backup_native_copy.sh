#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

client_opts=(
  --allow_repeated_settings
  --send_logs_level 'error'
)

use_s3_plain_rewriteable_as_db_disk=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name='disk_db_remote' AND type = 'ObjectStorage' AND object_storage_type='S3' AND metadata_type='PlainRewritable'" | tr -d '[:space:]')

# When using s3_plain_rewriteable as a db_disk, even when allow_s3_native_copy=false, we still copy metadata files.
if [ "$use_s3_plain_rewriteable_as_db_disk" = "1" ]; then
    metadata_s3_copy_object_events_without_s3_native_copy=2
else
    metadata_s3_copy_object_events_without_s3_native_copy=0
fi

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree() order by tuple() settings disk='s3_disk';
    insert into data select * from numbers(10);
"

# BACKUP
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "BACKUP TABLE data TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_native_copy') SETTINGS allow_s3_native_copy=true"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "BACKUP TABLE data TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_no_native_copy') SETTINGS allow_s3_native_copy=false"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"

# BACKUP incremental
# NOTE: due to base_backup S3CopyObject should be 0 here
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "BACKUP TABLE data TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_native_copy_inc') SETTINGS allow_s3_native_copy=true, base_backup=S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_native_copy')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "BACKUP TABLE data TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_no_native_copy_inc') SETTINGS allow_s3_native_copy=false, base_backup=S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_no_native_copy')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"

# RESTORE
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "RESTORE TABLE data AS data_native_copy FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_native_copy') SETTINGS allow_s3_native_copy=true"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "RESTORE TABLE data AS data_no_native_copy FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_no_native_copy') SETTINGS allow_s3_native_copy=false"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>$metadata_s3_copy_object_events_without_s3_native_copy FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"
# RESTORE from incremental backup
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "RESTORE TABLE data AS data_native_copy_inc FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_native_copy_inc') SETTINGS allow_s3_native_copy=true"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"
query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$query_id" -q "RESTORE TABLE data AS data_no_native_copy_inc FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_no_native_copy_inc') SETTINGS allow_s3_native_copy=false"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT query, ProfileEvents['S3CopyObject']>$metadata_s3_copy_object_events_without_s3_native_copy FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"
