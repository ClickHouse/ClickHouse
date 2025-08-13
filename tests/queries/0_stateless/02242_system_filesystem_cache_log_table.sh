#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for STORAGE_POLICY in 's3_cache' 'local_cache' 'azure_cache'; do
    echo "Using storage policy: $STORAGE_POLICY"

    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"

    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_2242"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS system.filesystem_cache_log"
    $CLICKHOUSE_CLIENT --query "CREATE TABLE test_2242 (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='$STORAGE_POLICY', min_bytes_for_wide_part = 10485760, compress_marks=false, compress_primary_key=false"
    $CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES test_2242"
    $CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=0 --enable_filesystem_cache_log=1 --query "INSERT INTO test_2242 SELECT number, toString(number) FROM numbers(100000)"

    $CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=0 --enable_filesystem_cache_log=1 --query "SELECT 2242, '$STORAGE_POLICY', * FROM test_2242 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
    $CLICKHOUSE_CLIENT --query "SELECT file_segment_range, read_type FROM system.filesystem_cache_log WHERE query_id = (SELECT query_id from system.query_log where query LIKE '%SELECT 2242%$STORAGE_POLICY%' AND current_database = currentDatabase() AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1) ORDER BY file_segment_range, read_type"

    $CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=0 --enable_filesystem_cache_log=1 --query "SELECT 2243, '$STORAGE_POLICY', * FROM test_2242 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
    $CLICKHOUSE_CLIENT --query "SELECT file_segment_range, read_type FROM system.filesystem_cache_log WHERE query_id = (SELECT query_id from system.query_log where query LIKE '%SELECT 2243%$STORAGE_POLICY%' AND current_database = currentDatabase() AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1) ORDER BY file_segment_range, read_type"

done
