#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings, no-flaky-check
# no-flaky-check: Too slow

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

for disk in 's3_disk' 'local_disk' 'azure'; do
    echo "Using disk: $disk"

    cache_path=02241_filesystem_cache_on_write_operations_$disk
    $CLICKHOUSE_CLIENT --echo --query "DROP TABLE IF EXISTS test_02241"
    $CLICKHOUSE_CLIENT --echo --query "CREATE TABLE test_02241 (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS disk=disk(type='cache', path='$cache_path', disk='$disk', max_size=10_000_000_000, cache_on_write_operations=1), min_bytes_for_wide_part = 10485760, compress_marks=false, compress_primary_key=false, min_bytes_for_full_part_storage=0, ratio_of_defaults_for_sparse_serialization = 1, write_marks_for_substreams_in_compact_parts=1, serialization_info_version='basic'"
    $CLICKHOUSE_CLIENT --echo --query "SYSTEM STOP MERGES test_02241"
    cache_name=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.disks WHERE cache_path LIKE '%$cache_path'")

    echo "SYSTEM CLEAR FILESYSTEM CACHE"
    drop_filesystem_cache $cache_name

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
        WHERE caches.cache_name = '$cache_name'
    )
    WHERE endsWith(local_path, 'data.bin')
    FORMAT Vertical"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path WHERE caches.cache_name = '$cache_name'"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

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
        WHERE caches.cache_name = '$cache_name'
    )
    WHERE endsWith(local_path, 'data.bin')
    FORMAT Vertical"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path WHERE caches.cache_name = '$cache_name'"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --echo --query "SELECT * FROM test_02241 FORMAT Null"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0 AND cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --echo --query "SELECT * FROM test_02241 FORMAT Null"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0 AND cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) size FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    echo "SYSTEM CLEAR FILESYSTEM CACHE"
    drop_filesystem_cache $cache_name

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
        WHERE caches.cache_name = '$cache_name'
    )
    WHERE endsWith(local_path, 'data.bin')
    FORMAT Vertical;"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path WHERE caches.cache_name = '$cache_name'"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"
    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(100) SETTINGS enable_filesystem_cache_on_write_operations=0"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(100)"
    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(300, 10000)"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --echo --query "SYSTEM START MERGES test_02241"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "OPTIMIZE TABLE test_02241 FINAL"

    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --mutations_sync=2 --query "ALTER TABLE test_02241 UPDATE value = 'kek' WHERE key = 100"
    $CLICKHOUSE_CLIENT --echo --query "SELECT count(), sum(size) FROM system.filesystem_cache WHERE cache_name = '$cache_name'"
    $CLICKHOUSE_CLIENT --echo --enable_filesystem_cache_on_write_operations=1 --query "INSERT INTO test_02241 SELECT number, toString(number) FROM numbers(5000000)"

    $CLICKHOUSE_CLIENT --echo --query "SYSTEM FLUSH LOGS query_log"

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
done |& sed 's/__tmp_internal_[0-9]*/__tmp_internal/'
