#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# no-random-merge-tree-settings: Wide packed parts with unhashed long file names make each insert take seconds.

# A skip index condition must not copy every column named like a map key subcolumn (`attrs.key_*`)
# when the predicate reads no map. The tables differ only in such names, so the same filter over
# 128 `bloom_filter` indexes must peak at about the same memory on both.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

num_indexes=128
num_wide_columns=1000
name_padding=$(printf 'p%.0s' $(seq 1 150))

indexed_columns=$(seq 1 $num_indexes | awk '{printf ", s%d String", $1}')
indexes=$(seq 1 $num_indexes | awk '{printf ", INDEX i%d s%d TYPE bloom_filter GRANULARITY 1", $1, $1}')
insert_columns=$(seq 1 $num_indexes | awk '{printf ", s%d", $1}')
insert_values=$(seq 1 $num_indexes | awk '{printf ", %cx%c", 39, 39}')
predicate=$(seq 1 $num_indexes | awk '{printf "%ss%d = %cx%c", ($1 == 1 ? "" : " AND "), $1, 39, 39}')

wide_columns()
{
    seq 1 $num_wide_columns | awk -v word="$1" -v padding="$name_padding" '{printf ", `attrs.%s_%d_%s` String", word, $1, padding}'
}

peak_memory_usage()
{
    local log
    log=$($CLICKHOUSE_CLIENT --max_untracked_memory 1 --print-profile-events --profile-events-delay-ms=-1 \
        -q "SELECT count() FROM $1 WHERE $predicate SETTINGS use_skip_indexes = 1, enable_parallel_replicas = 0 FORMAT Null" 2>&1)
    grep -vE '\[ [0-9]+ \] [A-Za-z]+: -?[0-9]+ \((increment|gauge)\)$' <<< "$log" >&2
    sed -nE 's/.*\[ 0 \] MemoryTrackerPeakUsage: ([0-9]+).*/\1/p' <<< "$log"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_plain_names"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_map_key_names"

echo "CREATE TABLE t_plain_names (id UInt64, attrs Map(String, String) $indexed_columns $(wide_columns val) $indexes) ENGINE = MergeTree ORDER BY id" | $CLICKHOUSE_CLIENT --max_query_size 10000000
echo "CREATE TABLE t_map_key_names (id UInt64, attrs Map(String, String) $indexed_columns $(wide_columns key) $indexes) ENGINE = MergeTree ORDER BY id" | $CLICKHOUSE_CLIENT --max_query_size 10000000

$CLICKHOUSE_CLIENT -q "INSERT INTO t_plain_names (id $insert_columns) SELECT 1 $insert_values"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_map_key_names (id $insert_columns) SELECT 1 $insert_values"

plain_peak=$(peak_memory_usage t_plain_names)
map_key_peak=$(peak_memory_usage t_map_key_names)

if [ $((map_key_peak - plain_peak)) -lt $((16 << 20)) ]; then
    echo "OK"
else
    echo "Peak memory usage: $((map_key_peak >> 20)) MiB with attrs.key_* names, $((plain_peak >> 20)) MiB with attrs.val_* names"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_plain_names"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_map_key_names"
