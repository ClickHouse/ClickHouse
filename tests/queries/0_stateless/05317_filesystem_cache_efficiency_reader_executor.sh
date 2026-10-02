#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# no-random-merge-tree-settings: the checks assume that a full scan returns every byte it caches. Some random
# MergeTree settings (for example `enable_block_number_column`, `prewarm_mark_cache`) make readers revisit and
# predownload segments, which leaves a few percent of the downloaded bytes unread.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Per-segment read coverage of the filesystem cache with `use_reader_executor = 1`: `active_bytes` and `windows_since_touch`
# in `system.filesystem_cache`. A private cache keeps other cache users out of the result. Its
# efficiency window (`efficiency_window_sec`, default 600 s) counts from cache creation, so the
# whole test runs inside window 0.
cache_name="cache_efficiency_executor_${CLICKHOUSE_DATABASE}"

disk="disk(
    type = cache,
    name = '$cache_name',
    path = '$cache_name/',
    max_size = '1Gi',
    max_file_segment_size = '4Mi',
    boundary_alignment = '4Mi',
    background_download_threads = 0,
    cache_on_write_operations = 1,
    load_metadata_asynchronously = 0,
    disk = 'local_disk')"

# Strictly synchronous reads that always go through the cache. A read counts at least one read
# buffer, so pin a small one: with the cache, `filesystem_cache_prefer_bigger_buffer_size` would
# raise it to `prefetch_buffer_size`, and a short forward seek would read through the gap.
read_settings=(
    --max_threads 1
    --enable_filesystem_cache 1
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0
    --filesystem_cache_allow_background_download 0
    --filesystem_cache_max_download_size 137438953472
    --remote_filesystem_read_prefetch 0
    --allow_prefetched_read_pool_for_remote_filesystem 0
    --use_reader_executor 1
    --reader_executor_block_size 131072
    --reader_executor_window_size 131072
    --reader_executor_min_bytes_for_seek 0
    --filesystem_cache_prefer_bigger_buffer_size 0
    --max_read_buffer_size_remote_fs 65536
    --remote_read_min_bytes_for_seek 0
    --remote_filesystem_read_method read
)

summary="
    SELECT count() > 0, countIf(windows_since_touch = 0) = count(), sum(active_bytes) >= 0.99 * sum(downloaded_size)
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_efficiency_executor;
    DROP TABLE IF EXISTS t_efficiency_executor_write;
    CREATE TABLE t_efficiency_executor (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS disk = $disk, index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
    CREATE TABLE t_efficiency_executor_write (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS disk = $disk, min_bytes_for_wide_part = 0;
    SYSTEM STOP MERGES t_efficiency_executor;
    SYSTEM STOP MERGES t_efficiency_executor_write;
    INSERT INTO t_efficiency_executor SELECT number, cityHash64(number) FROM numbers(2000000);
"

# 1. A full scan reads every byte that it caches.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(key), sum(value) FROM t_efficiency_executor FORMAT Null"
$CLICKHOUSE_CLIENT --query "$summary"

# 2. SLRU: the first fill is probationary, a second read moves the data segments to protected.
# Marks come from the mark cache on the second read, so their small segments stay probationary.
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT queue_entry_type FROM system.filesystem_cache WHERE cache_name = '$cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(key), sum(value) FROM t_efficiency_executor FORMAT Null"
$CLICKHOUSE_CLIENT --query "
    SELECT sumIf(downloaded_size, queue_entry_type = 'SLRU_Protected') > 0.9 * sum(downloaded_size)
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

# 3. Point queries read a small part of each 4 MiB cell.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "
    SELECT sum(value) FROM t_efficiency_executor
    WHERE key IN (17, 200017, 400017, 600017, 800017, 1000017, 1200017, 1400017, 1600017, 1800017)
    FORMAT Null"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0, countIf(windows_since_touch = 0) = count(), sum(active_bytes) < 0.5 * sum(downloaded_size)
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

# 4. Write-through puts data into the cache without a read.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations 1 \
    --query "INSERT INTO t_efficiency_executor_write SELECT number, cityHash64(number) FROM numbers(500000)"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(windows_since_touch IS NULL) > 0, sum(active_bytes) * 100 < sum(downloaded_size)
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_efficiency_executor;
    DROP TABLE t_efficiency_executor_write;
"
