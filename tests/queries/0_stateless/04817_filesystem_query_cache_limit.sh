#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `filesystem_cache_max_download_size` limits how much a single query writes into the filesystem
# cache in total, not how much a single space reservation takes. The two tables hold the same data
# and neither is cached before its read, so the only difference is the limit.

disk_name="04817_query_limit_${CLICKHOUSE_DATABASE}"
limit=1048576

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test_no_limit;
DROP TABLE IF EXISTS test_with_limit;
CREATE TABLE test_no_limit (key UInt32, value String)
ENGINE = MergeTree() ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, disk = disk(
    type = cache,
    name = '${disk_name}',
    path = '${disk_name}',
    max_size = '100Mi',
    max_file_segment_size = '64Ki',
    boundary_alignment = '64Ki',
    background_download_threads = 0,
    background_download_queue_size_limit = 0,
    load_metadata_asynchronously = 0,
    enable_filesystem_query_cache_limit = 1,
    disk = disk(type = object_storage, object_storage_type = local, metadata_type = local, path = '${disk_name}_data/'));
CREATE TABLE test_with_limit AS test_no_limit;

SET enable_filesystem_cache_on_write_operations = 0;
INSERT INTO test_no_limit SELECT number, toString(rand64()) FROM numbers(500000);
INSERT INTO test_with_limit SELECT number, toString(rand64()) FROM numbers(500000);
"

# How much a query writes into the cache is `CachedReadBufferCacheWriteBytes`, which is what the
# limit bounds. The size of the whole cache is not a substitute: it also holds what other queries
# (and previous runs of this one) put there.
written_bytes() {
    $CLICKHOUSE_CLIENT -m --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['CachedReadBufferCacheWriteBytes']
    FROM system.query_log WHERE query_id = '$1' AND type = 'QueryFinish' AND current_database = currentDatabase()
    ORDER BY event_time_microseconds DESC LIMIT 1;"
}

$CLICKHOUSE_CLIENT --query "SELECT * FROM test_no_limit FORMAT Null" --query_id "no_limit_${CLICKHOUSE_DATABASE}"
echo "no limit: writes more than the limit  $(( $(written_bytes "no_limit_${CLICKHOUSE_DATABASE}") > limit ))"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test_with_limit FORMAT Null" --query_id "with_limit_${CLICKHOUSE_DATABASE}" --filesystem_cache_max_download_size $limit
echo "with limit: stays within the limit  $(( $(written_bytes "with_limit_${CLICKHOUSE_DATABASE}") <= limit ))"

$CLICKHOUSE_CLIENT -m --query "DROP TABLE test_no_limit; DROP TABLE test_with_limit;"
