#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan
# It's not clear why distributed aggregation is much slower with sanitizers (https://github.com/ClickHouse/ClickHouse/issues/60625)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

function were_parallel_replicas_used () {
    $CLICKHOUSE_CLIENT --query "
        SELECT
            initial_query_id,
            concat('Used parallel replicas: ', (ProfileEvents['ParallelReplicasUsedCount'] > 0)::bool::String) as used
        FROM system.query_log
    WHERE event_date >= yesterday()
      AND initial_query_id LIKE '$1%'
      AND query_id = initial_query_id
      AND type = 'QueryFinish'
      AND current_database = '$CLICKHOUSE_DATABASE'
    ORDER BY event_time_microseconds ASC
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

    # $1 -> query_id
    # $2 -> parallel_replicas_min_number_of_rows_per_replica
    # $3 -> query
function run_query_with_pure_parallel_replicas () {
    # Note that we look into the logs to know how many parallel replicas were estimated because, although the coordinator
    # might decide to use N replicas, one of them might be fast and do all the work before others start up. This means
    # that those replicas wouldn't log into the system.query_log and the test would be flaky

    $CLICKHOUSE_CLIENT_TRACE \
        --query "$3" \
        --query_id "${1}_pure" \
        --max_parallel_replicas 3 \
        --prefer_localhost_replica 1 \
        --parallel_replicas_prefer_local_join 0 \
        --cluster_for_parallel_replicas "parallel_replicas" \
        --enable_parallel_replicas 1 \
        --parallel_replicas_for_non_replicated_merge_tree 1 \
        --parallel_replicas_min_number_of_rows_per_replica "$2" \
    |& grep "It is enough work for" | awk '{ print substr($7, 2, length($7) - 2) "\t" $20 " estimated parallel replicas" }' | sort -n -k2 -b | grep -Pv "\t0 estimated parallel replicas"
}

query_id_base="02784_automatic_parallel_replicas_join-$CLICKHOUSE_DATABASE"


#### JOIN (left side 10M, right side 1M)
#### As the right side of the JOIN is a table and not a subquery, ideally the right side should be left untouched and
#### pushed down into each replica. This isn't implemented yet and the right side of the join is being transformed into
#### a subquery, which then is executed in parallel (https://github.com/ClickHouse/ClickHouse/issues/49301#issuecomment-1619897920)
#### This is why when we print estimation it happens twice, once for each side of the join
simple_join_query="SELECT sum(value) FROM test_parallel_replicas_automatic_left_side INNER JOIN test_parallel_replicas_automatic_count_right_side USING number format Null"

# With 0 rows we won't have any estimation (no logs either). Both queries will be executed in parallel
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_0" 0 "$simple_join_query"

# Once a limit is set we get estimation. One message for each part of the join (see message above)
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_10M" 10000000 "$simple_join_query" # Right: 0. Left: 1->0
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_5M" 5000000 "$simple_join_query" # Right: 0. Left: 2
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_1M" 1000000 "$simple_join_query" # Right: 1->0. Left: 10->3
run_query_with_pure_parallel_replicas "${query_id_base}_simple_join_300k" 300000 "$simple_join_query" # Right: 2. Left: 33->3

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
were_parallel_replicas_used "${query_id_base}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_parallel_replicas_automatic_left_side"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_parallel_replicas_automatic_count_right_side"
