#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings, no-flaky-check
# Tag no-flaky-check -- access to system.remote_data_path is too slow with thread fuzzer enabled

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

for STORAGE_POLICY in 's3_cache' 'local_cache' 'azure_cache'; do
    echo "Using storage policy: $STORAGE_POLICY"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_02286"

    $CLICKHOUSE_CLIENT --query "CREATE TABLE test_02286 (key UInt32, value String)
                                   Engine=MergeTree()
                                   ORDER BY key
                                   SETTINGS storage_policy='$STORAGE_POLICY', min_bytes_for_wide_part = 10485760"

    $CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES test_02286"
    drop_filesystem_cache

    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache"
    $CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=0 --query "INSERT INTO test_02286 SELECT number, toString(number) FROM numbers(100)"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02286 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache"

    drop_filesystem_cache
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02286 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --multiline --query "SYSTEM DROP FILESYSTEM CACHE 'ff'; --{serverError 36}"

    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02286 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache"
    $CLICKHOUSE_CLIENT --query "SELECT count()
                                   FROM (
                                       SELECT
                                           arrayJoin(cache_paths) AS cache_path,
                                           local_path,
                                           remote_path
                                       FROM
                                           system.remote_data_paths
                                       ) AS data_paths
                                   INNER JOIN system.filesystem_cache AS caches
                                   ON data_paths.cache_path = caches.cache_path"

    $CLICKHOUSE_CLIENT --query "DROP TABLE test_02286 SYNC"

    cache_entries=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache")
    echo "$cache_entries"
    # system.remote_data_paths is very slow for web disks, so let's avoid extra
    # call to it (we need it only for debugging of this tests, and only when we
    # have cache entries, which tests does not expect)
    if [ $cache_entries -gt 0 ]; then
        $CLICKHOUSE_CLIENT --query "SELECT cache_path FROM system.filesystem_cache"
        $CLICKHOUSE_CLIENT --query "SELECT cache_path, local_path
                                       FROM (
                                           SELECT
                                               arrayJoin(cache_paths) AS cache_path,
                                               local_path,
                                               remote_path
                                           FROM
                                               system.remote_data_paths
                                           ) AS data_paths
                                       INNER JOIN system.filesystem_cache AS caches
                                       ON data_paths.cache_path = caches.cache_path "
    fi

    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_022862"
done
