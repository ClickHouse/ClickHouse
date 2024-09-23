#!/usr/bin/env bash
# Tags: no-random-settings, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="02963_remote_read_bug"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test;

CREATE TABLE test (a Int32, s String)
ENGINE = MergeTree()
ORDER BY a
SETTINGS disk = disk(name = '$disk_name', type = cache, max_size = '10Gi', path = '$disk_name', disk = 's3_disk');

INSERT INTO test SELECT number % 1000000, randomString(1) FROM numbers_mt(1e7) SETTINGS enable_filesystem_cache_on_write_operations = 0;

OPTIMIZE TABLE test FINAL;
"

query_id=$(random_str 10)

$CLICKHOUSE_CLIENT -m --query_id "$query_id" --query "
WITH RANDOM_SET AS (
    SELECT rand32() % 10000 FROM numbers(100)
)
SELECT *
FROM test
WHERE a IN RANDOM_SET AND s IN ('x', 'y', 'z')
FORMAT Null
SETTINGS
    max_threads = 10,
    allow_prefetched_read_pool_for_remote_filesystem = 1, filesystem_prefetch_min_bytes_for_single_read_task = '1Ki',
    merge_tree_min_bytes_for_concurrent_read = 1, merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 1, merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 1;
"

$CLICKHOUSE_CLIENT -m --query "
SYSTEM FLUSH LOGS;

-- This threshold was determined experimentally - before the fix this ratio had values around 50K
SELECT throwIf(ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] / ProfileEvents['WriteBufferFromFileDescriptorWrite'] < 200000)
FROM system.query_log
WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$query_id' AND type = 'QueryFinish';
"

