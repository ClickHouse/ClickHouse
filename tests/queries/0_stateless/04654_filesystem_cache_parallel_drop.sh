#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-distributed-cache
# Tag no-fasttest: requires S3 disk
# Tag no-parallel: uses SYSTEM DROP FILESYSTEM CACHE
# Tag no-distributed-cache: reads go through the distributed cache and do not populate the local filesystem cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="04654_filesystem_cache_parallel_drop"

$CLICKHOUSE_CLIENT -m --query """
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk_name', type = cache, max_size = '100Mi', path = '$disk_name', disk = 's3_disk', drop_cache_threads = 4);

INSERT INTO test SELECT number, randomString(100) FROM numbers(100000);
"""

$CLICKHOUSE_CLIENT --query "SELECT drop_cache_threads FROM system.filesystem_cache_settings WHERE cache_name = '$disk_name'"

$CLICKHOUSE_CLIENT -m --query """
SYSTEM DROP FILESYSTEM CACHE '$disk_name';
SELECT count() FROM test WHERE NOT ignore(*)
SETTINGS enable_filesystem_cache = 1, optimize_trivial_count_query = 0, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0;
SELECT count() > 0 FROM system.filesystem_cache WHERE cache_name = '$disk_name';
SYSTEM DROP FILESYSTEM CACHE '$disk_name';
SELECT count() FROM system.filesystem_cache WHERE cache_name = '$disk_name';
"""

# Concurrent drops share the cache's bounded drop_cache_pool: the request whose
# workers do not fit into the queue blocks in scheduling until the other request's
# workers finish. Both must complete without hanging or throwing.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM test WHERE NOT ignore(*) FORMAT Null
SETTINGS enable_filesystem_cache = 1, optimize_trivial_count_query = 0, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0"

$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$disk_name'" &
first_drop=$!
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$disk_name'" &
second_drop=$!
wait $first_drop
first_drop_result=$?
wait $second_drop
second_drop_result=$?
[ $first_drop_result -eq 0 ] && [ $second_drop_result -eq 0 ] && echo "concurrent drops OK"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache WHERE cache_name = '$disk_name'"

$CLICKHOUSE_CLIENT --query "DROP TABLE test"
