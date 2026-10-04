#!/usr/bin/env bash
# Tags: no-object-storage, no-random-settings, no-random-merge-tree-settings, no-fasttest
# no-fasttest: The test is slow

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists data;
    create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
"

# Reading 2e5*8 bytes at 160000 B/s takes 1.6e6/160000-1 = 9 seconds (-1 is the 1s token burst).
# The cap was 1e6 B/s over 1e6 rows, which owed exactly 7 seconds - the same value the
# query_duration_ms assertion below demands, leaving the sleep assertion no margin.
# The throttler only sleeps while the arrival rate exceeds the cap, so the cap must stay far
# below the natural read rate or the sleep assertion flaps on loaded runners (seen in #113107).
$CLICKHOUSE_CLIENT -q "insert into data select * from numbers(2e5)"

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
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "select * from data format Null settings max_local_read_bandwidth=160000, local_filesystem_read_method='$read_method'"
    $CLICKHOUSE_CLIENT -m -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT
            '$read_method',
            query_duration_ms >= 7e3,
            ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] > 1.6e6,
            ProfileEvents['QueryLocalReadThrottlerBytes'] > 1.6e6,
            ProfileEvents['QueryLocalReadThrottlerSleepMicroseconds'] > 7e6*0.5
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id' AND type != 'QueryStart'
    "
done
