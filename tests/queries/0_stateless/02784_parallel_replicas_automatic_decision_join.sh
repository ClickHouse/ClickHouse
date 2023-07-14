#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function involved_parallel_replicas () {
    # Not using current_database = '$CLICKHOUSE_DATABASE' as nested parallel queries aren't run with it
    $CLICKHOUSE_CLIENT --query "
        SELECT
            initial_query_id,
            (count() - 2) / 2 as number_of_parallel_replicas
        FROM system.query_log
    WHERE event_date >= yesterday()
      AND initial_query_id LIKE '$1%'
    GROUP BY initial_query_id
    ORDER BY min(event_time_microseconds) ASC
    FORMAT TSV"
}

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE IF NOT EXISTS test_parallel_replicas_automatic_left_side
    (
        number Int64,
        p Int64
    )
    ENGINE=MergeTree()
      ORDER BY number
      PARTITION BY p
      SETTINGS index_granularity = 8192  -- Don't randomize it to avoid flakiness
    AS
      SELECT number, number % 2 AS p FROM numbers(2_000_000)
      UNION ALL
      SELECT number, 3 AS p FROM numbers(10_000_000, 8_000_000)
"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE IF NOT EXISTS test_parallel_replicas_automatic_count_right_side
    (
        number Int64,
        value Int64
    )
    ENGINE=MergeTree()
      ORDER BY number
      SETTINGS index_granularity = 8192  -- Don't randomize it to avoid flakiness
    AS
      SELECT number, number % 2 AS v FROM numbers(1_000_000)
"

function run_query_with_pure_parallel_replicas () {
    # $1 -> query_id
    # $2 -> min rows per replica
    # $3 -> query
    $CLICKHOUSE_CLIENT \
        --query "$3" \
        --query_id "${1}_pure" \
        --max_parallel_replicas 3 \
        --prefer_localhost_replica 1 \
        --use_hedged_requests 0 \
        --cluster_for_parallel_replicas 'parallel_replicas' \
        --allow_experimental_parallel_reading_from_replicas 1 \
        --parallel_replicas_for_non_replicated_merge_tree 1 \
        --parallel_replicas_min_number_of_rows_per_replica "$2"
}

query_id_base="02784_automatic_parallel_replicas_join-$CLICKHOUSE_DATABASE"


#### JOIN (left side 10M, right side 1M)
#### As the right side of the JOIN is a table, ideally it shouldn't be executed with parallel replicas and instead passed as is to the replicas
#### so each of them executes the join with the assigned granules of the left table, but that's not implemented yet
#### https://github.com/ClickHouse/ClickHouse/issues/49301#issuecomment-1619897920
#### Note that this currently fails with the analyzer since it doesn't support JOIN with parallel replicas
simple_join_query="SELECT sum(value) FROM test_parallel_replicas_automatic_left_side INNER JOIN test_parallel_replicas_automatic_count_right_side USING number format Null"
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_0" 0 "$simple_join_query" # 3 replicas for the right side first, 3 replicas for the left
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_10M" 10000000 "$simple_join_query" # Right: 0. Left: 0
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_5M" 5000000 "$simple_join_query" # Right: 0. Left: 2
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_1M" 1000000 "$simple_join_query" # Right: 1->0. Left: 10->3
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_300k" 400000 "$simple_join_query" # Right: 2. Left: 3

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
involved_parallel_replicas "${query_id_base}"

$CLICKHOUSE_CLIENT --query "DROP TABLE test_parallel_replicas_automatic_left_side"
$CLICKHOUSE_CLIENT --query "DROP TABLE test_parallel_replicas_automatic_count_right_side"
