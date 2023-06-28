#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-s3-storage, no-random-settings

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for STORAGE_POLICY in 's3_cache' 'local_cache'; do
    echo "Using storage policy: $STORAGE_POLICY"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FILESYSTEM CACHE"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_02240_storage_policy"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_02240_storage_policy (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='${STORAGE_POLICY}', min_bytes_for_wide_part = 10485760, compress_marks=false, compress_primary_key=false"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES test_02240_storage_policy"
    ${CLICKHOUSE_CLIENT} --enable_filesystem_cache_on_write_operations=0 --query "INSERT INTO test_02240_storage_policy SELECT number, toString(number) FROM numbers(100)"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache ORDER BY file_segment_range_end, size"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FILESYSTEM CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FILESYSTEM CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_02240_storage_policy_3"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_02240_storage_policy_3 (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='${STORAGE_POLICY}_3', min_bytes_for_wide_part = 10485760, compress_marks=false, compress_primary_key=false"
    ${CLICKHOUSE_CLIENT} --enable_filesystem_cache_on_write_operations=0 --query "INSERT INTO test_02240_storage_policy_3 SELECT number, toString(number) FROM numbers(100)"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy_3 FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache ORDER BY file_segment_range_end, size"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy_3 FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache ORDER BY file_segment_range_end, size"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FILESYSTEM CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache"
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_02240_storage_policy_3 FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FILESYSTEM CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache"
done
