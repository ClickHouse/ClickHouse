#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings, no-flaky-check
# Tag no-flaky-check -- access to system.remote_data_path is too slow with thread fuzzer enabled

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

set -e

function wait_for_cache_cleanup()
{
    local max_tries=30
    # Wait for all queries to finish (query may still be running if a thread is killed by timeout)
    local num_tries=0
    while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$1'") -ne 0 ]]; do
        sleep 1;
        num_tries=$((num_tries+1))
        if [ $num_tries -eq $max_tries ]; then
            break
        fi
    done
}

for disk in 's3_disk' 'local_disk' 'azure'; do
    echo "Using disk: $disk"

    cache_path=drop_filesystem_cache_test_$disk
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_02286"
    $CLICKHOUSE_CLIENT --query "CREATE TABLE test_02286 (key UInt32, value String)
                                Engine=MergeTree()
                                ORDER BY key
                                SETTINGS disk=disk(type='cache', path='$cache_path', disk='$disk', max_size=10_000_000_000, cache_on_write_operations=1), min_bytes_for_wide_part = 10485760"

    cache_name=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.disks WHERE cache_path LIKE '%$cache_path'")

    $CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES test_02286"
    drop_filesystem_cache $cache_name

    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'"
    $CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=0 --query "INSERT INTO test_02286 SELECT number, toString(number) FROM numbers(100)"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02286 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    drop_filesystem_cache $cache_name
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02286 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --multiline --query "SYSTEM CLEAR FILESYSTEM CACHE 'ff'; --{serverError 36}"

    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02286 FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'"
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
                                   ON data_paths.cache_path = caches.cache_path
                                   WHERE caches.cache_name = '$cache_name'"

    $CLICKHOUSE_CLIENT --query "DROP TABLE test_02286 SYNC"

    wait_for_cache_cleanup "$cache_name"
    cache_entries=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$cache_name'")
    echo "$cache_entries"
    # system.remote_data_paths is very slow for web disks, so let's avoid extra
    # call to it (we need it only for debugging of this tests, and only when we
    # have cache entries, which tests does not expect)
    if [ $cache_entries -gt 0 ]; then
        $CLICKHOUSE_CLIENT --query "SELECT cache_path FROM system.filesystem_cache WHERE cache_name = '$cache_name'"
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
                                       ON data_paths.cache_path = caches.cache_path
                                       WHERE caches.cache_name = '$cache_name'"
    fi

    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_022862"
done
