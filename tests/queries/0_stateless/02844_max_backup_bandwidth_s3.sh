#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

client_opts=(
  --allow_repeated_settings
  --send_logs_level 'error'
)

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    drop table if exists data;
    create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, disk='s3_disk';
    -- 2e6*8 bytes = 16MB, with 1MB/s bandwidth it should take ~16 seconds for non-native copy
    insert into data select * from numbers(2e6);
"

query_id_native=$(random_str 10)
$CLICKHOUSE_CLIENT "${client_opts[@]}" --query_id "$query_id_native" -q "backup table data to S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data/backup2') SETTINGS allow_s3_native_copy=1" --max_backup_bandwidth=1M > /dev/null

query_id_no_native=$(random_str 10)
$CLICKHOUSE_CLIENT "${client_opts[@]}" --query_id "$query_id_no_native" -q "backup table data to S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data/backup3') SETTINGS allow_s3_native_copy=0" --max_backup_bandwidth=1M > /dev/null

# Check that non-native copy with bandwidth limiting takes significantly longer than native copy.
# Native copy uses S3 server-side copy which bypasses client bandwidth limiting.
# We check that non-native copy is at least 10 seconds longer than native copy,
# which verifies that bandwidth limiting is being applied.
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        (no_native_duration_ms - native_duration_ms) >= 10000 AS bandwidth_limiting_works
    FROM (
        SELECT
            (SELECT query_duration_ms FROM system.query_log
             WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id_native' AND type != 'QueryStart') AS native_duration_ms,
            (SELECT query_duration_ms FROM system.query_log
             WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id_no_native' AND type != 'QueryStart') AS no_native_duration_ms
    )
"
