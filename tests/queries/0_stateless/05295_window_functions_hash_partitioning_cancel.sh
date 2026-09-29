#!/usr/bin/env bash
# `query_plan_window_functions_hash_partitioning`: a query killed once all its rows are read, while the partitions
# of the deferred rows are grouped and the results are made, must stop and free the aggregate states. The query
# may also finish before it is killed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NUM_ROWS=3000000
SETTINGS="query_plan_window_functions_hash_partitioning = 1, query_plan_reuse_storage_ordering_for_window_functions = 0, max_threads = 1"

function kill_after_reading()
{
    local name=$1
    local query=$2
    local query_id="${CLICKHOUSE_DATABASE}_$name"

    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "$query SETTINGS $SETTINGS" > /dev/null 2>&1 &
    local pid=$!

    # Until all rows are read, or the query has finished.
    while kill -0 $pid 2>/dev/null; do
        read_rows=$($CLICKHOUSE_CLIENT -q "SELECT read_rows FROM system.processes WHERE query_id = '$query_id'")
        [[ "$read_rows" == "$NUM_ROWS" ]] && break
        sleep 0.01
    done
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
    wait $pid

    $CLICKHOUSE_CLIENT -q "SELECT '$name', count() FROM system.processes WHERE query_id = '$query_id'"
}

# One row in each partition.
kill_after_reading one_row_in_each_partition \
    "SELECT sum(s), sum(u), sum(length(g)) FROM (SELECT sum(number) OVER w AS s, uniqExact(number) OVER w AS u, groupArray(number) OVER w AS g FROM numbers($NUM_ROWS) WINDOW w AS (PARTITION BY number))"

# Three rows in each partition: the results are made for each partition.
kill_after_reading three_rows_in_each_partition \
    "SELECT sum(s), sum(u), sum(length(g)) FROM (SELECT sum(number) OVER w AS s, uniqExact(number) OVER w AS u, groupArray(number) OVER w AS g FROM numbers($NUM_ROWS) WINDOW w AS (PARTITION BY intDiv(number, 3)))"

# The server is fine, and the same queries finish when they are not killed.
$CLICKHOUSE_CLIENT -q "SELECT sum(s), sum(u), sum(length(g)) FROM (SELECT sum(number) OVER w AS s, uniqExact(number) OVER w AS u, groupArray(number) OVER w AS g FROM numbers(300000) WINDOW w AS (PARTITION BY intDiv(number, 3))) SETTINGS $SETTINGS"
