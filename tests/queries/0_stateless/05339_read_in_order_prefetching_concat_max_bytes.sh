#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The read-ahead budget of `PrefetchingConcatProcessor` (`read_in_order_max_bytes_to_prefetch`)
# must not affect the order or the content of the result, whether the budget is exhausted
# immediately (0, 1 byte), partway, or never.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="read_in_order_two_level_merge_threshold = 100,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    optimize_aggregation_in_order = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    enable_parallel_replicas = 0, max_threads = 8, max_block_size = 1000, optimize_read_in_order = 1"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_prefetching_concat_max_bytes (key String, value UInt64)
    ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 1024
    AS SELECT concat('k/', toString(number % 100000), '/v'), number FROM numbers(1000000);
    OPTIMIZE TABLE t_prefetching_concat_max_bytes FINAL;"

$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_max_bytes WHERE key LIKE '%/v' ORDER BY key SETTINGS $SETTINGS)
    WHERE trimLeft(explain) LIKE 'PrefetchingConcat%'"

for budget in 0 1 100000 1000000000
do
    $CLICKHOUSE_CLIENT -q "SELECT * FROM t_prefetching_concat_max_bytes WHERE key LIKE '%/v' ORDER BY key
        SETTINGS $SETTINGS, read_in_order_max_bytes_to_prefetch = $budget" > "${CLICKHOUSE_TMP}/result_${budget}.tsv"
    echo "$budget $(wc -l < "${CLICKHOUSE_TMP}/result_${budget}.tsv")"
    LC_ALL=C sort -c -s -t $'\t' -k1,1 "${CLICKHOUSE_TMP}/result_${budget}.tsv" && echo "sorted"
    cmp -s "${CLICKHOUSE_TMP}/result_0.tsv" "${CLICKHOUSE_TMP}/result_${budget}.tsv" && echo "same as with no budget"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_prefetching_concat_max_bytes"
