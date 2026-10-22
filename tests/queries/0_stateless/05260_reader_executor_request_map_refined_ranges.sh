#!/usr/bin/env bash
# Tags: no-object-storage, no-distributed-cache, no-encrypted-storage, no-parallel-replicas
# The test finds the column file by its name in the logged path, which object storage replaces with a random key.
# The executor falls back to the legacy read path for the distributed cache and for decryption, so nothing is announced.
# Parallel replicas read with more than one replica, so the maps depend on the assignment of the coordinator.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Small compressed blocks, so that the blocks of the three granules that the query reads are a small part of the file.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_refined (k UInt64, v UInt64, INDEX v_minmax v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1,
        min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, min_compress_block_size = 4096, max_compress_block_size = 4096;
    INSERT INTO t_refined SELECT number, number FROM numbers(100000);
    OPTIMIZE TABLE t_refined FINAL;
"
# A case-insensitive disk stores the file under the hash of the stream name.
read -r file_name file_size <<< "$($CLICKHOUSE_CLIENT -q "
    SELECT filenames[1], column_data_compressed_bytes FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_refined' AND column = 'v' AND active")"

# The key analysis cannot use the filter on `v`, so the part keeps all its granules until the refiner of the pool applies
# the skip index to each task and leaves the three granules that hold the values. With `max_rows_to_read` the index is
# applied in the analysis instead, for the row estimate.
map_sizes=$($CLICKHOUSE_CLIENT --send_logs_level=test --use_reader_executor=1 --max_threads=1 \
    --remote_filesystem_read_method=read --local_filesystem_read_method=pread --enable_filesystem_cache=0 \
    --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0 \
    --use_skip_indexes=1 --use_skip_indexes_on_data_read=1 --use_indexes_refiner_in_read_pools=1 --use_query_condition_cache=0 \
    --max_rows_to_read=0 \
    -q "SELECT sum(k) FROM t_refined WHERE v BETWEEN 50000 AND 50200" 2>&1 >/dev/null \
    | grep -o "Request map of [^ ]*/$file_name\.bin: [0-9]* bytes" | grep -o '[0-9]* bytes$' | grep -o '[0-9]*')

# The map of a task leaves out the granules the refiner has dropped from the part.
echo "maps announced: $([ -n "$map_sizes" ] && echo 1 || echo 0)"
echo "every map is under a tenth of the file: $(echo "$map_sizes" | awk -v size="$file_size" '$1 * 10 >= size { wide = 1 } END { print wide ? 0 : 1 }')"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_refined"
