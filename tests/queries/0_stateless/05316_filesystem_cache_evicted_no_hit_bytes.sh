#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FilesystemCacheEvictedNoHitBytes` counts bytes evicted from file segments that the cache never served.
# The private LRU cache holds one table but not two. With SLRU, new segments compete only inside
# the probationary queue, so the query evicts its own new segments.
cache_name="cache_no_hit_${CLICKHOUSE_DATABASE}"

disk="disk(
    type = cache,
    name = '$cache_name',
    path = '$cache_name/',
    max_size = '12Mi',
    max_file_segment_size = '1Mi',
    boundary_alignment = '1Mi',
    cache_policy = 'LRU',
    keep_free_space_size_ratio = 0,
    keep_free_space_elements_ratio = 0,
    background_download_threads = 0,
    cache_on_write_operations = 0,
    load_metadata_asynchronously = 0,
    disk = 'local_disk')"

read_settings=(
    --max_threads 1
    --enable_parallel_replicas 0
    --use_uncompressed_cache 0
    --remote_filesystem_read_method read
    --enable_filesystem_cache 1
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0
    --filesystem_cache_allow_background_download 0
    --filesystem_cache_max_download_size 137438953472
    --remote_filesystem_read_prefetch 0
    --allow_prefetched_read_pool_for_remote_filesystem 0
    --use_reader_executor 0
)

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_no_hit_a;
    DROP TABLE IF EXISTS t_no_hit_b;
    CREATE TABLE t_no_hit_a (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = $disk, min_bytes_for_wide_part = 0;
    CREATE TABLE t_no_hit_b (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = $disk, min_bytes_for_wide_part = 0;
    SYSTEM STOP MERGES t_no_hit_a;
    SYSTEM STOP MERGES t_no_hit_b;
    INSERT INTO t_no_hit_a SELECT cityHash64(number) FROM numbers(1000000);
    INSERT INTO t_no_hit_b SELECT cityHash64(number) FROM numbers(1000000);
    SYSTEM DROP FILESYSTEM CACHE '$cache_name';
"

# Read A, then B: B evicts segments of A that the cache never served.
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(x) FROM t_no_hit_a FORMAT Null"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query_id "${CLICKHOUSE_DATABASE}_b_first" --query "SELECT sum(x) FROM t_no_hit_b FORMAT Null"
# Start again and read B twice, so its data segments get a hit and are the oldest entries. Then A evicts them.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(x) FROM t_no_hit_b FORMAT Null"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(x) FROM t_no_hit_b FORMAT Null"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query_id "${CLICKHOUSE_DATABASE}_a_second" --query "SELECT sum(x) FROM t_no_hit_a FORMAT Null"

# The second value is not exactly 0: mark files of B are read once into the mark cache and get no hit.
$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    WITH
        (SELECT ProfileEvents['FilesystemCacheEvictedNoHitBytes'] FROM system.query_log
         WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_DATABASE}_b_first' AND type = 'QueryFinish') AS first,
        (SELECT ProfileEvents['FilesystemCacheEvictedNoHitBytes'] FROM system.query_log
         WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_DATABASE}_a_second' AND type = 'QueryFinish') AS second
    SELECT first > 0, second * 10 < first;
    DROP TABLE t_no_hit_a;
    DROP TABLE t_no_hit_b;
"
