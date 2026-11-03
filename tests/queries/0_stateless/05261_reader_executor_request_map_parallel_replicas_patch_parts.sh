#!/usr/bin/env bash
# Tags: no-object-storage, no-distributed-cache, no-encrypted-storage
# The test finds the patch part by its directory in the logged path, which object storage replaces with a random key.
# The executor falls back to the legacy read path for the distributed cache and for decryption, so nothing is announced.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Small compressed blocks, so that the byte ranges of separate mark ranges of the patch part do not merge.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_replicas_patched (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1,
        min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, min_compress_block_size = 1024,
        max_compress_block_size = 1024, enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';
    INSERT INTO t_replicas_patched SELECT number, number FROM numbers(100000);
    OPTIMIZE TABLE t_replicas_patched FINAL;
"
# One patch row for every tenth row, so the patch part has granules over the whole key range.
$CLICKHOUSE_CLIENT --enable_lightweight_update=1 -q "UPDATE t_replicas_patched SET v = k + 1 WHERE k % 10 = 0"

# A case-insensitive disk stores the file under the hash of the stream name.
read -r patch_file_name patch_file_size <<< "$($CLICKHOUSE_CLIENT -q "
    SELECT filenames[1], column_data_compressed_bytes FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_replicas_patched' AND column = 'k' AND active AND startsWith(name, 'patch-')")"

# The coordinator assigns the part to the replicas in portions of about 160 marks of the 782.
map_sizes=$($CLICKHOUSE_CLIENT --send_logs_level=test --use_reader_executor=1 --max_threads=1 --apply_patch_parts=1 \
    --remote_filesystem_read_method=read --local_filesystem_read_method=pread --enable_filesystem_cache=0 \
    --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0 \
    --enable_parallel_replicas=1 --automatic_parallel_replicas_mode=0 --max_parallel_replicas=3 --parallel_replicas_for_non_replicated_merge_tree=1 \
    --cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost \
    --merge_tree_min_rows_for_concurrent_read=20480 --merge_tree_min_bytes_for_concurrent_read=251658240 \
    --parallel_replicas_mark_segment_size=128 \
    -q "SELECT sum(v) FROM t_replicas_patched" 2>&1 >/dev/null \
    | grep -o "Request map of [^ ]*/patch-[^/]*/$patch_file_name\.bin: [0-9]* bytes" | grep -o '[0-9]* bytes$' | grep -o '[0-9]*')

# The patch readers of each replica announce the patch ranges of what the coordinator assigned to it.
echo "patch maps announced: $([ -n "$map_sizes" ] && echo 1 || echo 0)"
echo "every patch map is smaller than the patch file: $(echo "$map_sizes" | awk -v size="$patch_file_size" '$1 >= size { whole = 1 } END { print whole ? 0 : 1 }')"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_replicas_patched"
