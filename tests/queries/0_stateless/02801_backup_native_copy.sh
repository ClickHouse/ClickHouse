#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree() order by tuple() settings disk='s3_disk';
    insert into data select * from numbers(10);
"

query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null --query_id $query_id -q "BACKUP TABLE data TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_native_copy') SETTINGS allow_s3_native_copy=true"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"

query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null --query_id $query_id -q "BACKUP TABLE data TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_no_native_copy') SETTINGS allow_s3_native_copy=false"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"

query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null --query_id $query_id -q "RESTORE TABLE data AS data_native_copy FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_native_copy') SETTINGS allow_s3_native_copy=true"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"

query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null --query_id $query_id -q "RESTORE TABLE data AS data_no_native_copy FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data_no_native_copy') SETTINGS allow_s3_native_copy=false"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT query, ProfileEvents['S3CopyObject']>0 FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id'
"
