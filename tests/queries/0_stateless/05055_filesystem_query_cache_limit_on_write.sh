#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query which writes through the cache without reading from it is bound by
# `filesystem_cache_query_limit_bytes` as well.

disk_name="05055_query_limit_on_write_${CLICKHOUSE_DATABASE}"
limit=524288

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String)
ENGINE = MergeTree() ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, disk = disk(
    type = cache,
    name = '${disk_name}',
    path = '${disk_name}',
    max_size = '100Mi',
    max_file_segment_size = '64Ki',
    boundary_alignment = '64Ki',
    cache_on_write_operations = 1,
    background_download_threads = 0,
    background_download_queue_size_limit = 0,
    load_metadata_asynchronously = 0,
    enable_filesystem_query_cache_limit = 1,
    disk = disk(type = object_storage, object_storage_type = local, metadata_type = local, path = '${disk_name}_data/'));
"

query_id="write_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "
INSERT INTO test SELECT number, toString(rand64()) FROM numbers(300000)
SETTINGS enable_filesystem_cache_on_write_operations = 1, max_insert_threads = 1,
         filesystem_cache_query_limit_bytes = ${limit}"

written=$($CLICKHOUSE_CLIENT -m --query "
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['CachedWriteBufferCacheWriteBytes']
FROM system.query_log
WHERE query_id = '${query_id}' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC LIMIT 1")

# The insert writes several megabytes, so it fills the whole budget and must stop there.
echo "write-through respects the query limit  $(( written > limit / 2 && written <= limit ))"

$CLICKHOUSE_CLIENT --query "DROP TABLE test"
