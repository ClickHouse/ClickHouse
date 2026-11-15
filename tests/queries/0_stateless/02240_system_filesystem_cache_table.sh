#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

for disk in 's3_disk' 'local_disk' 'azure'; do
    echo "Using disk: $disk"

    cache_path=02240_system_filesystem_cache_table_$disk
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_02240_storage_policy"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE test_02240_storage_policy (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY key
        SETTINGS disk=disk(type='cache', path='$cache_path', disk='$disk', max_size=10_000_000_000, cache_on_write_operations=1), min_bytes_for_wide_part = 1000000, compress_marks=false, compress_primary_key=false, serialization_info_version='basic'
    "
    cache_name=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.disks WHERE cache_path LIKE '%$cache_path'")

    drop_filesystem_cache $cache_name
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR MARK CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES test_02240_storage_policy"
    ${CLICKHOUSE_CLIENT} --enable_filesystem_cache_on_write_operations=0 --query "INSERT INTO test_02240_storage_policy SELECT number, toString(number) FROM numbers(100)"

    echo 'Expect cache'
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR MARK CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT state, file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '$cache_name' ORDER BY file_segment_range_begin, file_segment_range_end, size"
    ${CLICKHOUSE_CLIENT} --query "SELECT uniqExact(key) FROM system.filesystem_cache WHERE cache_name = '$cache_name'";

    echo 'Expect cache'
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR MARK CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT state, file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '$cache_name' ORDER BY file_segment_range_begin, file_segment_range_end, size"
    ${CLICKHOUSE_CLIENT} --query "SELECT uniqExact(key) FROM system.filesystem_cache WHERE cache_name = '$cache_name'";

    drop_filesystem_cache $cache_name
    echo 'Expect no cache'
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    echo 'Expect cache'
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR MARK CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT state, file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '$cache_name' ORDER BY file_segment_range_begin, file_segment_range_end, size"
    ${CLICKHOUSE_CLIENT} --query "SELECT uniqExact(key) FROM system.filesystem_cache WHERE cache_name = '$cache_name'";

    drop_filesystem_cache $cache_name
    echo 'Expect no cache'
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

done
