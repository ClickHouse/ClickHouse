#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: requires a cache disk.
# no-random-settings, no-random-merge-tree-settings: a narrow read must leave its file segments
# partially downloaded; randomized read buffers, prefetch and compression blocks change that.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

cache_name="05394_slru_partial_${CLICKHOUSE_DATABASE}"
qid_prefix="05394_${CLICKHOUSE_DATABASE}"

# SLRU with 4 MiB protected and 4 MiB probationary queues. 1 MiB file segments, no background
# download, so a narrow read leaves a file segment partially downloaded.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (key UInt64 CODEC(NONE), value UInt64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY key
    SETTINGS disk = disk(
        type = cache,
        name = '$cache_name',
        path = '$cache_name',
        disk = 'local_disk',
        max_size = '8Mi',
        cache_policy = 'SLRU',
        slru_size_ratio = 0.5,
        max_file_segment_size = '1Mi',
        boundary_alignment = '1Mi',
        background_download_threads = 0,
        cache_on_write_operations = 0),
    min_bytes_for_wide_part = 0,
    index_granularity = 8192,
    min_compress_block_size = 65536,
    max_compress_block_size = 65536"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t"
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT number, number FROM numbers(1000000)"
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
    --enable_parallel_replicas 0
)

function query()
{
    local qid=$1
    local condition=$2
    $CLICKHOUSE_CLIENT "${read_settings[@]}" --query_id "${qid_prefix}_${qid}" -q "SELECT sum(value) FROM t WHERE $condition"
}

# The first granule is at the start of the first file segment of each column.
query first_1 "key < 100"
$CLICKHOUSE_CLIENT -q "
    SELECT 'partially downloaded after the first read', count() >= 2
    FROM system.filesystem_cache
    WHERE cache_name = '$cache_name' AND state = 'PARTIALLY DOWNLOADED' AND file_segment_range_begin = 0"

# The second hit promotes the partially downloaded segments to the protected queue.
query first_2 "key < 100"
downloaded_after_promotion=$($CLICKHOUSE_CLIENT -q "
    SELECT sum(downloaded_size)
    FROM system.filesystem_cache
    WHERE cache_name = '$cache_name' AND state = 'PARTIALLY DOWNLOADED' AND file_segment_range_begin = 0")

# The second granule is after the downloaded prefix of the same segments: they grow in the protected queue.
query second_1 "key >= 8192 AND key < 8292"

# A scan of other file segments goes through the probationary queue and evicts all of it.
query scan "key >= 300000" > /dev/null

# The protected queue kept the promoted segments with the bytes they got after the promotion.
$CLICKHOUSE_CLIENT -q "
    SELECT 'kept after the scan', count(), sum(downloaded_size) > $downloaded_after_promotion
    FROM system.filesystem_cache
    WHERE cache_name = '$cache_name' AND state = 'PARTIALLY DOWNLOADED' AND file_segment_range_begin = 0"

# The mark cache is shared with other tests. A read that misses it reloads each mark file through
# this cache, which can promote the mark file's segment, so mark file loads are subtracted.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(query_id, '${qid_prefix}_', '') AS read,
        ProfileEvents['FilesystemCachePromotedFileSegments'] - ProfileEvents['LoadedMarksFiles'] >= 2 AS promoted,
        ProfileEvents['CachedReadBufferReadFromSourceBytes'] = 0 AS all_from_cache,
        ProfileEvents['FilesystemCacheEvictedFileSegments'] > 0 AS evicted
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id IN ('${qid_prefix}_first_2', '${qid_prefix}_scan')
    ORDER BY event_time_microseconds"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
