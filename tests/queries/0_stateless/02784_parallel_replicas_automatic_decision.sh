#!/usr/bin/env bash
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

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_parallel_replicas_automatic_count"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE IF NOT EXISTS test_parallel_replicas_automatic_count
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
        --cluster_for_parallel_replicas "parallel_replicas" \
        --enable_parallel_replicas 1 \
        --parallel_replicas_for_non_replicated_merge_tree 1 \
        --parallel_replicas_min_number_of_rows_per_replica "$2" \
        --max_threads 5 \
    |& grep "It is enough work for" | awk '{ print substr($7, 2, length($7) - 2) "\t" $20 " estimated parallel replicas" }'
}

query_id_base="02784_automatic_parallel_replicas-$CLICKHOUSE_DATABASE"

#### Reading 10M rows without filters
whole_table_query="SELECT sum(number) FROM test_parallel_replicas_automatic_count format Null"
run_query_with_pure_parallel_replicas "${query_id_base}_whole_table_10M" 10000000 "$whole_table_query"
run_query_with_pure_parallel_replicas "${query_id_base}_whole_table_6M" 6000000 "$whole_table_query" # 1.6 replicas -> 1 replica -> No parallel replicas
run_query_with_pure_parallel_replicas "${query_id_base}_whole_table_5M" 5000000 "$whole_table_query"
run_query_with_pure_parallel_replicas "${query_id_base}_whole_table_1M" 1000000 "$whole_table_query"

##### Reading 2M rows without filters as partition (p=3) is pruned completely
query_with_partition_pruning="SELECT sum(number) FROM test_parallel_replicas_automatic_count WHERE p != 3 format Null"
run_query_with_pure_parallel_replicas "${query_id_base}_pruning_10M" 10000000 "$query_with_partition_pruning"
run_query_with_pure_parallel_replicas "${query_id_base}_pruning_1M" 1000000 "$query_with_partition_pruning"
run_query_with_pure_parallel_replicas "${query_id_base}_pruning_500k" 500000 "$query_with_partition_pruning"

#### Reading ~500k rows as index filter should prune granules from partition=1 and partition=2, and drop p3 completely
query_with_index="SELECT sum(number) FROM test_parallel_replicas_automatic_count WHERE number < 500_000 format Null"
run_query_with_pure_parallel_replicas "${query_id_base}_index_1M" 1000000 "$query_with_index"
run_query_with_pure_parallel_replicas "${query_id_base}_index_200k" 200000 "$query_with_index"
run_query_with_pure_parallel_replicas "${query_id_base}_index_100k" 100000 "$query_with_index"

#### Reading 1M (because of LIMIT)
limit_table_query="SELECT sum(number) FROM (SELECT number FROM test_parallel_replicas_automatic_count LIMIT 1_000_000) format Null"
run_query_with_pure_parallel_replicas "${query_id_base}_limit_10M" 10000000 "$limit_table_query"
run_query_with_pure_parallel_replicas "${query_id_base}_limit_1M" 1000000 "$limit_table_query"
run_query_with_pure_parallel_replicas "${query_id_base}_limit_500k" 500000 "$limit_table_query"

#### Reading 10M (because of LIMIT is applied after aggregations)
limit_agg_table_query="SELECT sum(number) FROM test_parallel_replicas_automatic_count LIMIT 1_000_000 format Null"
run_query_with_pure_parallel_replicas "${query_id_base}_useless_limit_500k" 500000 "$limit_agg_table_query"

#### If the filter does not help, it shouldn't disable parallel replicas. Table has 10M rows, filter removes all rows
helpless_filter_query="SELECT sum(number) FROM test_parallel_replicas_automatic_count WHERE number + p = 42 format Null"
run_query_with_pure_parallel_replicas "${query_id_base}_helpless_filter_10M" 10000000 "$helpless_filter_query"
run_query_with_pure_parallel_replicas "${query_id_base}_helpless_filter_5M" 5000000 "$helpless_filter_query"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
were_parallel_replicas_used "${query_id_base}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_parallel_replicas_automatic_count"
