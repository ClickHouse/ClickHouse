#!/usr/bin/env bash
# Tags: no-object-storage, no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists data;
    create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
"

# reading 1e6*8 bytes with 1M bandwith it should take (8-1)/1=7 seconds
$CLICKHOUSE_CLIENT -q "insert into data select * from numbers(1e6)"

query_id=$(random_str 10)
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "backup table data to Disk('backups', '$CLICKHOUSE_DATABASE/data/backup1')" --max_backup_bandwidth=1M > /dev/null
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS;
    SELECT
        query_duration_ms >= 7e3,
        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] > 8e6
    FROM system.query_log
    WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id' AND type != 'QueryStart'
"
