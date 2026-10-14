#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: requires a cache disk.
# no-random-settings, no-random-merge-tree-settings: a narrow read must leave its file segment
# partially downloaded; randomized read buffers, prefetch and compression blocks change that.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

cache_name="05395_slru_partial_large_${CLICKHOUSE_DATABASE}"

# SLRU with a 1 MiB protected queue and a 3 MiB probationary queue. A 2 MiB file segment does not
# fit the protected queue whole. No background download, so a narrow read leaves it partially downloaded.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (key UInt64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY key
    SETTINGS disk = disk(
        type = cache,
        name = '$cache_name',
        path = '$cache_name',
        disk = 'local_disk',
        max_size = '4Mi',
        cache_policy = 'SLRU',
        slru_size_ratio = 0.25,
        max_file_segment_size = '2Mi',
        boundary_alignment = '2Mi',
        background_download_threads = 0,
        cache_on_write_operations = 0),
    min_bytes_for_wide_part = 0,
    index_granularity = 8192,
    min_compress_block_size = 65536,
    max_compress_block_size = 65536"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t"
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT number FROM numbers(1000000)"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"

read_settings=(
    --max_threads 1
    --filesystem_cache_prefer_bigger_buffer_size 0
    --max_read_buffer_size 65536
    --max_read_buffer_size_remote_fs 65536
    --remote_read_min_bytes_for_seek 0
    --remote_filesystem_read_method read
    --remote_filesystem_read_prefetch 0
    --use_uncompressed_cache 0
    --use_query_condition_cache 0
    --use_reader_executor 0
)

# Two narrow reads leave the first file segment partially downloaded and hit it. It is not promoted,
# because it cannot grow in the protected queue to its full size.
$CLICKHOUSE_CLIENT "${read_settings[@]}" -q "SELECT sum(key) FROM t WHERE key < 100"
$CLICKHOUSE_CLIENT "${read_settings[@]}" -q "SELECT sum(key) FROM t WHERE key < 100"

# The first 32 granules cover the first file segment. It stays in the probationary queue, where it fits,
# so the download completes.
$CLICKHOUSE_CLIENT "${read_settings[@]}" -q "SELECT sum(key) FROM t WHERE key < 262144"

$CLICKHOUSE_CLIENT -q "
    SELECT 'first file segment', state, downloaded_size
    FROM system.filesystem_cache
    WHERE cache_name = '$cache_name' AND file_segment_range_begin = 0 AND size = 2097152"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
