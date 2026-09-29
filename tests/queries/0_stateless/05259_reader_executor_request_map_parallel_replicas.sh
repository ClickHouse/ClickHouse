#!/usr/bin/env bash
# Tags: no-object-storage, no-distributed-cache, no-encrypted-storage
# The test finds the column file by its name in the logged path, which object storage replaces with a random key.
# The executor falls back to the legacy read path for the distributed cache and for decryption, so nothing is announced.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_replicas (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
        replace_long_file_name_to_hash = 0;
    INSERT INTO t_replicas SELECT number FROM numbers(1000000);
    OPTIMIZE TABLE t_replicas FINAL;
"
file_size=$($CLICKHOUSE_CLIENT -q "
    SELECT column_data_compressed_bytes FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_replicas' AND column = 'k' AND active")

# The coordinator assigns the part to the replicas in portions of about 160 marks of the 977.
map_sizes=$($CLICKHOUSE_CLIENT --send_logs_level=test --use_reader_executor=1 --max_threads=1 \
    --remote_filesystem_read_method=read --local_filesystem_read_method=pread --enable_filesystem_cache=0 \
    --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0 \
    --enable_parallel_replicas=1 --automatic_parallel_replicas_mode=0 --max_parallel_replicas=3 --parallel_replicas_for_non_replicated_merge_tree=1 \
    --cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost \
    --merge_tree_min_rows_for_concurrent_read=163840 --merge_tree_min_bytes_for_concurrent_read=251658240 \
    --parallel_replicas_mark_segment_size=128 \
    -q "SELECT sum(k) FROM t_replicas" 2>&1 >/dev/null | grep -o 'Request map of [^ ]*/k\.bin: [0-9]* bytes' | grep -o '[0-9]* bytes$' | grep -o '[0-9]*')

# Each replica announces what the coordinator assigned to it, not all ranges of the part.
echo "maps announced: $([ -n "$map_sizes" ] && echo 1 || echo 0)"
echo "every map is smaller than the file: $(echo "$map_sizes" | awk -v size="$file_size" '$1 >= size { whole = 1 } END { print whole ? 0 : 1 }')"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_replicas"
