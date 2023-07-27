#!/usr/bin/env bash
# Tags: no-s3-storage, no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data;
    create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
"

# reading 1e6*8 bytes with 1M bandwith it should take (8-1)/1=7 seconds
$CLICKHOUSE_CLIENT -q "insert into data select * from numbers(1e6)"

read_methods=(
    read
    pread
    pread_threadpool
    # NOTE: io_uring doing all IO from one thread, that is not attached to the query
    # io_uring
    # NOTE: mmap cannot be throttled
    # mmap
)
for read_method in "${read_methods[@]}"; do
    query_id=$(random_str 10)
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "select * from data format Null settings max_local_read_bandwidth='1M', local_filesystem_read_method='$read_method'"
    $CLICKHOUSE_CLIENT -nm -q "
        SYSTEM FLUSH LOGS;
        SELECT
            '$read_method',
            query_duration_ms >= 7e3,
            ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] > 8e6,
            ProfileEvents['LocalReadThrottlerBytes'] > 8e6,
            ProfileEvents['LocalReadThrottlerSleepMicroseconds'] > 7e6*0.5
        FROM system.query_log
        WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id' AND type != 'QueryStart'
    "
done
