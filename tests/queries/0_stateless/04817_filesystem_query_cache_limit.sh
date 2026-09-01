#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `filesystem_cache_query_limit_bytes` limits how much a single query writes into the filesystem
# cache in total, not how much a single space reservation takes. The two tables hold the same amount of data
# and neither is cached before its read, so the only difference is the limit.

disk_name="04817_query_limit_${CLICKHOUSE_DATABASE}"
limit=1048576

# Both are randomized in CI, and the wrong value of either makes a read cache nothing, so pin them.
cache_settings="enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0"

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
SET max_insert_threads = 1;
INSERT INTO test_no_limit SELECT number, toString(rand64()) FROM numbers(500000);
INSERT INTO test_with_limit SELECT number, toString(rand64()) FROM numbers(500000);
SYSTEM STOP MERGES test_no_limit;
SYSTEM STOP MERGES test_with_limit;
"

# How much a query writes into the cache is `CachedReadBufferCacheWriteBytes`, which is what the
# limit bounds. The size of the whole cache is not a substitute: it also holds what other queries
# (and previous runs of this one) put there.
written_bytes() {
    $CLICKHOUSE_CLIENT -m --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT max(ProfileEvents['CachedReadBufferCacheWriteBytes'])
    FROM (SELECT ProfileEvents FROM system.query_log
          WHERE query_id = '$1' AND type = 'QueryFinish' AND current_database = currentDatabase()
          ORDER BY event_time_microseconds DESC LIMIT 1);"
}

$CLICKHOUSE_CLIENT --query_id "no_limit_${CLICKHOUSE_DATABASE}" \
    --query "SELECT * FROM test_no_limit SETTINGS ${cache_settings} FORMAT Null"
echo "no limit: writes more than the limit  $(( $(written_bytes "no_limit_${CLICKHOUSE_DATABASE}") > limit ))"

$CLICKHOUSE_CLIENT --query_id "with_limit_${CLICKHOUSE_DATABASE}" \
    --query "SELECT * FROM test_with_limit
             SETTINGS ${cache_settings}, filesystem_cache_query_limit_bytes = ${limit} FORMAT Null"
echo "with limit: stays within the limit  $(( $(written_bytes "with_limit_${CLICKHOUSE_DATABASE}") <= limit ))"

$CLICKHOUSE_CLIENT -m --query "DROP TABLE test_no_limit; DROP TABLE test_with_limit;"
