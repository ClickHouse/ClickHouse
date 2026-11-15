#!/usr/bin/env bash
# Tags: no-parallel-replicas
# The sliced pool is used for local reading only.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A cancel with partial_result_on_first_cancel ends the sources while the router still waits for
# their slices. The router has to end its lanes instead of waiting forever ("Pipeline stuck").

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_sliced_cancel;
CREATE TABLE t_sliced_cancel (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_sliced_cancel;
INSERT INTO t_sliced_cancel SELECT number, number * 7 FROM numbers(0, 20000);
INSERT INTO t_sliced_cancel SELECT number, number * 7 FROM numbers(12000, 20000);
INSERT INTO t_sliced_cancel SELECT number, number * 7 FROM numbers(16000, 20000);
INSERT INTO t_sliced_cancel SELECT number, number * 7 FROM numbers(30000, 20000);
"

QUERY_ID="${CLICKHOUSE_DATABASE}_sliced_pool_partial_result"

# No row matches, so the whole table would be read; the sleep keeps the query running until the
# cancel arrives, with every source holding a slice.
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" --query "
SELECT k FROM t_sliced_cancel WHERE sleepEachRow(0.0001) = 0 AND v % 7 = 1 ORDER BY k LIMIT 10
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_sliced_pool = 1,
    read_in_order_two_level_merge_threshold = 100, optimize_move_to_prewhere = 0,
    max_threads = 4, max_block_size = 1024,
    merge_tree_min_rows_for_concurrent_read = 512, merge_tree_min_bytes_for_concurrent_read = 1,
    function_sleep_max_microseconds_per_block = 3000000, partial_result_on_first_cancel = 1
" &
pid=$!

for _ in {0..60}
do
    $CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$QUERY_ID'" | grep -F '1' && break
    sleep 0.5
done

kill -SIGINT $pid
wait $pid
echo "client exit code $?"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_sliced_cancel"
