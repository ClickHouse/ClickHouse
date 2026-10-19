#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings
# no-parallel: the failpoint below is server-global.
# no-fasttest: requires a cache disk.
# no-random-settings: randomized buffer sizes change how much data spans file segments.

# Regression for a LOGICAL_ERROR (server abort in debug/sanitizer builds) when the
# file_cache_dynamic_resize_fail_to_evict failpoint fired during ordinary space
# reservation. That failpoint models an eviction failure and is only meaningful for the
# dynamic cache resize feature, so it must not fire on the reserve path. See issue #88945.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CACHE_NAME="03908_cache_${CLICKHOUSE_DATABASE}"
QID="03908_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_03908"

# A per-database unique cache name/path means the cache starts empty. A small max_size
# plus incompressible values (randomString) and tiny compress blocks make the data span
# many small file segments, so the two write-through INSERTs together far exceed the
# cache and the second one is forced to evict on the reserve path by a wide margin.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_03908 (key UInt64, value String)
    ENGINE = MergeTree()
    ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0,
             min_compress_block_size = 4096,
             max_compress_block_size = 4096,
             disk = disk(
                type = cache,
                name = '$CACHE_NAME',
                path = '$CACHE_NAME/',
                disk = 'local_disk',
                max_size = '1Mi',
                max_file_segment_size = '100Ki',
                boundary_alignment = '100Ki',
                background_download_threads = 0,
                cache_on_write_operations = 1)"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_03908"

$CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=1 -q "INSERT INTO t_03908 SELECT number, randomString(2000) FROM numbers(5000)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict"
# The failpoint is server-global, so disable it on every exit path (set -e). Otherwise a
# failure below would leave it armed for later stateless tests on the same server.
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict"' EXIT

# This second insert forces cache eviction on the reserve path. Before the fix the
# failpoint fired here and aborted the server with a LOGICAL_ERROR. Now the failpoint
# is confined to dynamic resize, so eviction proceeds normally and the insert succeeds.
$CLICKHOUSE_CLIENT --query_id "$QID" --enable_filesystem_cache_on_write_operations=1 -q "INSERT INTO t_03908 SELECT number, randomString(2000) FROM numbers(5000, 5000)"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# The eviction really happened on the reserve path (not merely "the insert succeeded"):
# the second INSERT evicted at least one file segment.
$CLICKHOUSE_CLIENT -q "
    SELECT 'evicted on reserve path: ' || toString(ProfileEvents['FilesystemCacheEvictedFileSegments'] > 0)
    FROM system.query_log
    WHERE query_id = '$QID' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1"

# The server is still alive and the data is queryable.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_03908"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_03908"
