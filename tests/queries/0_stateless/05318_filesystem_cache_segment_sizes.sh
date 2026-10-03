#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The size distribution of the file segments of one cache: `file_segments_by_size` and
# `bytes_by_segment_size` in `system.filesystem_cache_settings` must match `system.filesystem_cache`.
# Private caches without background download or background eviction stay unchanged between the two reads.
cache_name="cache_segment_sizes_${CLICKHOUSE_DATABASE}"
small_cache_name="cache_segment_sizes_small_${CLICKHOUSE_DATABASE}"

disk_settings="
    max_file_segment_size = '32Mi',
    boundary_alignment = '4Mi',
    background_download_threads = 0,
    keep_free_space_size_ratio = 0,
    keep_free_space_elements_ratio = 0,
    load_metadata_asynchronously = 0,
    disk = 'local_disk'"

read_settings=(
    --max_threads 1
    --enable_parallel_replicas 0
    --enable_filesystem_cache 1
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0
    --filesystem_cache_allow_background_download 0
    --filesystem_cache_max_download_size 137438953472
    --use_uncompressed_cache 0
    --use_reader_executor 0
)

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_segment_sizes;
    DROP TABLE IF EXISTS t_segment_sizes_small;
    CREATE TABLE t_segment_sizes (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0,
        disk = disk(type = cache, name = '$cache_name', path = '$cache_name/', max_size = '1Gi', $disk_settings);
    CREATE TABLE t_segment_sizes_small (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0,
        disk = disk(type = cache, name = '$small_cache_name', path = '$small_cache_name/', max_size = '24Mi', $disk_settings);
    INSERT INTO t_segment_sizes SELECT number, cityHash64(number) FROM numbers(4000000);
    INSERT INTO t_segment_sizes_small SELECT number, cityHash64(number) FROM numbers(4000000);
    OPTIMIZE TABLE t_segment_sizes FINAL;
    OPTIMIZE TABLE t_segment_sizes_small FINAL;
    SYSTEM STOP MERGES t_segment_sizes;
    SYSTEM STOP MERGES t_segment_sizes_small;
"

# Whether the segment counts and the bytes per bucket of the cache match its file segments.
function check()
{
    local name=$1
    $CLICKHOUSE_CLIENT --query "
        WITH actual AS
        (
            SELECT
                multiIf(size <= 524288, '524288', size <= 1048576, '1048576', size <= 2097152, '2097152',
                    size <= 4194304, '4194304', size <= 8388608, '8388608', size <= 16777216, '16777216', 'inf') AS bucket,
                count() AS segments,
                sum(downloaded_size) AS bytes
            FROM system.filesystem_cache
            WHERE cache_name = '$name'
            GROUP BY bucket
        )
        SELECT
            arraySort(arrayFilter(x -> x.2 > 0, arrayZip(mapKeys(file_segments_by_size), mapValues(file_segments_by_size))))
                = (SELECT arraySort(groupArray((bucket, segments))) FROM actual),
            arraySort(arrayFilter(x -> x.2 > 0, arrayZip(mapKeys(bytes_by_segment_size), mapValues(bytes_by_segment_size))))
                = (SELECT arraySort(groupArray((bucket, bytes))) FROM actual)
        FROM system.filesystem_cache_settings
        WHERE cache_name = '$name'"
}

# 1. A scan fills the cache with long segments.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(key), sum(value) FROM t_segment_sizes FORMAT Null"
check "$cache_name"
$CLICKHOUSE_CLIENT --query "
    SELECT arraySum(mapValues(bytes_by_segment_size)) > 0 FROM system.filesystem_cache_settings WHERE cache_name = '$cache_name'"

# 2. Point reads.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "
    SELECT sum(value) FROM t_segment_sizes WHERE key IN (17, 1000017, 2000017, 3000017) FORMAT Null"
check "$cache_name"

# 3. Dropping the cache removes everything.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT --query "
    SELECT arraySum(mapValues(file_segments_by_size)), arraySum(mapValues(bytes_by_segment_size))
    FROM system.filesystem_cache_settings WHERE cache_name = '$cache_name'"

# 4. A scan larger than the cache evicts file segments; the counters stay consistent.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$small_cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(key), sum(value) FROM t_segment_sizes_small FORMAT Null"
check "$small_cache_name"

# 5. The asynchronous metrics have one key per bucket.
$CLICKHOUSE_CLIENT --query "
    SELECT metric, arraySort(mapKeys(key_values)) FROM system.asynchronous_metrics
    WHERE metric IN ('FilesystemCacheFileSegmentsBySize', 'FilesystemCacheBytesBySegmentSize')
    ORDER BY metric"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_segment_sizes;
    DROP TABLE t_segment_sizes_small;
"
