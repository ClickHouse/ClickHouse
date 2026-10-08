#!/usr/bin/env bash
# Tags: no-distributed-cache, no-encrypted-storage, no-parallel-replicas
# The executor falls back to the legacy read path for the distributed cache and for decryption, so nothing is announced.
# Parallel replicas learn their ranges from the coordinator only after the readers exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The distinct range counts of the request maps that the executor receives while it runs the query.
function range_counts()
{
    $CLICKHOUSE_CLIENT --send_logs_level=test --use_reader_executor=1 --max_threads=1 \
        --remote_filesystem_read_method=read --local_filesystem_read_method=pread --enable_filesystem_cache=0 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0 \
        --use_statistics_for_sum_avg_aggregation=0 \
        -q "$1" 2>&1 >/dev/null | grep -o 'Request map of [^ ]*: [0-9]* bytes in [0-9]* ranges' | grep -o '[0-9]* ranges$' | sort -u
}

# In ClickHouse Cloud a compact part keeps up to `compact_parts_max_granules_to_buffer` granules of each column together,
# and the map of a reader of all columns covers the whole group, so the two key ranges of `t_compact` would merge.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_wide (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    CREATE TABLE t_compact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_wide_part = '1G', min_bytes_for_full_part_storage = 0,
        compact_parts_max_granules_to_buffer = 1;
    CREATE TABLE t_packed (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = '1G';
"

for table in t_wide t_compact t_packed
do
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, number FROM numbers(100000)"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $table FINAL"

    # In a packed part, the view of each file first announces its whole slice of the archive: 1 ranges.
    echo "$table: two key ranges"
    range_counts "SELECT sum(v) FROM $table WHERE k < 5000 OR k >= 90000"
    echo "$table: full scan"
    range_counts "SELECT sum(v) FROM $table"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_wide; DROP TABLE t_compact; DROP TABLE t_packed"
