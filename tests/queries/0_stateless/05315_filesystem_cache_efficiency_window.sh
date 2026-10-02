#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# no-random-merge-tree-settings: check 1 expects a scan on an empty cache to have no cache hits. Some random
# MergeTree settings (for example `enable_block_number_column`, `prewarm_mark_cache`) make one scan revisit
# segments it already filled, and a revisit is a real cache hit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Per-segment reuse coverage of the filesystem cache: `active_bytes`, `passive_bytes`, `idle_bytes`
# in `system.filesystem_cache`. Only bytes served from the cache count; filling the cache does not.
# A private cache keeps other cache users out of the result. Its efficiency window
# (`efficiency_window_sec`, default 600 s) counts from cache creation, so the whole test runs
# inside window 0.
cache_name="cache_efficiency_${CLICKHOUSE_DATABASE}"

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

# Strictly synchronous reads that always go through the cache, also on a repeated read (no
# uncompressed cache). A cache hit counts at least one read buffer, so pin a small one: with the
# cache, `filesystem_cache_prefer_bigger_buffer_size` would raise it to `prefetch_buffer_size`,
# and a short forward seek would read through the gap.
read_settings=(
    --max_threads 1
    --enable_filesystem_cache 1
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0
    --filesystem_cache_allow_background_download 0
    --filesystem_cache_max_download_size 137438953472
    --remote_filesystem_read_prefetch 0
    --allow_prefetched_read_pool_for_remote_filesystem 0
    --use_uncompressed_cache 0
    --use_reader_executor 0
    --filesystem_cache_prefer_bigger_buffer_size 0
    --max_read_buffer_size_remote_fs 65536
    --remote_read_min_bytes_for_seek 0
    --remote_filesystem_read_method read
)

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_efficiency;
    DROP TABLE IF EXISTS t_efficiency_write;
    CREATE TABLE t_efficiency (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS disk = $disk, index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
    CREATE TABLE t_efficiency_write (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS disk = $disk, min_bytes_for_wide_part = 0;
    SYSTEM STOP MERGES t_efficiency;
    SYSTEM STOP MERGES t_efficiency_write;
    INSERT INTO t_efficiency SELECT number, cityHash64(number) FROM numbers(2000000);
"

# 1. A scan on an empty cache fills it. Filling is not reuse, so nothing is active yet.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(key), sum(value) FROM t_efficiency FORMAT Null"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0, sum(idle_bytes) = sum(downloaded_size), sum(active_bytes) = 0
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

# 2. SLRU: the first fill is probationary.
$CLICKHOUSE_CLIENT --query "SELECT DISTINCT queue_entry_type FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

# 3. A second scan reads from the cache: it reuses almost every byte of the segments it reads, and
# its hits move the data segments to protected. Marks come from the mark cache, so their small
# segments get no hit.
$CLICKHOUSE_CLIENT "${read_settings[@]}" --query "SELECT sum(key), sum(value) FROM t_efficiency FORMAT Null"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(active_bytes) > 0,
        sum(active_bytes) >= 0.99 * (sum(active_bytes) + sum(passive_bytes)),
        sum(active_bytes + passive_bytes + idle_bytes) = sum(downloaded_size)
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"
$CLICKHOUSE_CLIENT --query "
    SELECT sumIf(downloaded_size, queue_entry_type = 'SLRU_Protected') > 0.9 * sum(downloaded_size)
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

# 4. Point queries: the first run fills 4 MiB cells, the second reuses a small part of each.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
for _ in 1 2; do
    $CLICKHOUSE_CLIENT "${read_settings[@]}" --query "
        SELECT sum(value) FROM t_efficiency
        WHERE key IN (17, 200017, 400017, 600017, 800017, 1000017, 1200017, 1400017, 1600017, 1800017)
        FORMAT Null"
done
$CLICKHOUSE_CLIENT --query "
    SELECT sum(active_bytes) > 0,
        sum(active_bytes) < 0.5 * (sum(active_bytes) + sum(passive_bytes))
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

# 5. Write-through puts data into the cache without a read.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"
$CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations 1 \
    --query "INSERT INTO t_efficiency_write SELECT number, cityHash64(number) FROM numbers(500000)"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(idle_bytes) > 0, sum(active_bytes) * 100 < sum(downloaded_size)
    FROM system.filesystem_cache WHERE cache_name = '$cache_name'"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_efficiency;
    DROP TABLE t_efficiency_write;
"
