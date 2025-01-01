#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for STORAGE_POLICY in 's3_cache' 'local_cache' 'azure_cache'; do
    echo "Using storage policy: $STORAGE_POLICY"

    $CLICKHOUSE_CLIENT --echo --query "DROP TABLE IF EXISTS test_02241"
    $CLICKHOUSE_CLIENT --echo --query "CREATE TABLE test_02241 (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='$STORAGE_POLICY', min_bytes_for_wide_part = 10485760, compress_marks=false, compress_primary_key=false, ratio_of_defaults_for_sparse_serialization = 1"
    $CLICKHOUSE_CLIENT --echo --query "SYSTEM STOP MERGES test_02241"

    $CLICKHOUSE_CLIENT --echo --query "SYSTEM DROP FILESYSTEM CACHE"

    $CLICKHOUSE_CLIENT --echo --query "SELECT file_segment_range_begin, file_segment_range_end, size, state
    FROM
    (
        SELECT file_segment_range_begin, file_segment_range_end, size, state, local_path
        FROM
        (
            SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path
            FROM system.remote_data_paths
        ) AS data_paths
        INNER JOIN
            system.filesystem_cache AS caches
        ON data_paths.cache_path = caches.cache_path
    )
    WHERE endsWith(local_path, 'data.bin')
    FORMAT Vertical"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(100)"

    $CLICKHOUSE_CLIENT --echo --query "SELECT file_segment_range_begin, file_segment_range_end, size, state
    FROM
    (
        SELECT file_segment_range_begin, file_segment_range_end, size, state, local_path
        FROM
        (
            SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path
            FROM system.remote_data_paths
        ) AS data_paths
        INNER JOIN
            system.filesystem_cache AS caches
        ON data_paths.cache_path = caches.cache_path
    )
    WHERE endsWith(local_path, 'data.bin')
    FORMAT Vertical"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0"

    $CLICKHOUSE_CLIENT --echo --query "SELECT * FROM test_02241 FORMAT Null"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0"

    $CLICKHOUSE_CLIENT --echo --query "SELECT * FROM test_02241 FORMAT Null"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) size FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --echo --query "SYSTEM DROP FILESYSTEM CACHE"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(100, 200)"

    $CLICKHOUSE_CLIENT --echo --query "SELECT file_segment_range_begin, file_segment_range_end, size, state
    FROM
    (
        SELECT file_segment_range_begin, file_segment_range_end, size, state, local_path
        FROM
        (
            SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path
            FROM system.remote_data_paths
        ) AS data_paths
        INNER JOIN
            system.filesystem_cache AS caches
        ON data_paths.cache_path = caches.cache_path
    )
    WHERE endsWith(local_path, 'data.bin')
    FORMAT Vertical;"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"
    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(100) SETTINGS enable_filesystem_cache_on_write_operations=0"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(100)"
    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(300, 10000)"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --echo --query "SYSTEM START MERGES test_02241"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "OPTIMIZE TABLE test_02241 FINAL"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --mutations_sync=2 --query "ALTER TABLE test_02241 UPDATE value = 'kek' WHERE key = 100"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache"
    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(5000000)"

    $CLICKHOUSE_CLIENT --echo --query "SYSTEM FLUSH LOGS"

    $CLICKHOUSE_CLIENT --query "SELECT
        query, ProfileEvents['RemoteFSReadBytes'] > 0 as remote_fs_read
    FROM
        system.query_log
    WHERE
        query LIKE '%SELECT number, toString(number) FROM numbers(5000000)%'
        AND type = 'QueryFinish'
        AND current_database = currentDatabase()
    ORDER BY
        query_start_time
        DESC
    LIMIT 1"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM test_02241"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM test_02241 WHERE value LIKE '%010%'"
done
