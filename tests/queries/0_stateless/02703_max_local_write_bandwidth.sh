#!/usr/bin/env bash
# Tags: no-s3-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data;
    create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
"

query_id=$(random_str 10)
# writes 1e6*8 bytes with 1M bandwith it should take (8-1)/1=7 seconds
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "insert into data select * from numbers(1e6) settings max_local_write_bandwidth='1M'"
$CLICKHOUSE_CLIENT -nm -q "
    SYSTEM FLUSH LOGS;
    SELECT
        query_duration_ms >= 7e3,
        ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] > 8e6,
        ProfileEvents['LocalWriteThrottlerBytes'] > 8e6
        /* LocalWriteThrottlerSleepMicroseconds is too unreliable in debug build, but query_duration_ms is enough */
    FROM system.query_log
    WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id' AND type != 'QueryStart'
"
