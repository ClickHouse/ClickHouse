#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: CREATE AS SELECT is disabled

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function has_used_parallel_replicas () {
    # Not using current_database = '$CLICKHOUSE_DATABASE' as nested parallel queries aren't run with it
    $CLICKHOUSE_CLIENT --query "
        SELECT
            initial_query_id,
            if(count() != 2, 'Used parallel', 'Not parallel'),
            sumIf(read_rows, is_initial_query) as read_rows,
            sumIf(read_bytes, is_initial_query) as read_bytes
        FROM system.query_log
    WHERE event_date >= yesterday() and initial_query_id LIKE '$1%'
    GROUP BY initial_query_id
    ORDER BY min(event_time_microseconds) ASC
    FORMAT TSV"
}

function run_query_with_pure_parallel_replicas () {
    $CLICKHOUSE_CLIENT \
        --query "$2" \
        --query_id "${1}_disabled" \
        --max_parallel_replicas 1

    $CLICKHOUSE_CLIENT \
        --query "$2" \
        --query_id "${1}_pure" \
        --max_parallel_replicas 3 \
        --prefer_localhost_replica 1 \
        --cluster_for_parallel_replicas 'test_cluster_one_shard_three_replicas_localhost' \
        --enable_parallel_replicas 1 \
        --enable_analyzer 0

    $CLICKHOUSE_CLIENT \
        --query "$2" \
        --query_id "${1}_pure_analyzer" \
        --max_parallel_replicas 3 \
        --prefer_localhost_replica 1 \
        --cluster_for_parallel_replicas 'test_cluster_one_shard_three_replicas_localhost' \
        --enable_parallel_replicas 1 \
        --enable_analyzer 1
}

function run_query_with_custom_key_parallel_replicas () {
    $CLICKHOUSE_CLIENT \
        --query "$2" \
        --query_id "${1}_disabled" \
        --max_parallel_replicas 1

    $CLICKHOUSE_CLIENT \
        --query "$2" \
        --query_id "${1}_custom_key" \
        --max_parallel_replicas 3 \
        --parallel_replicas_mode 'custom_key_sampling' \
        --parallel_replicas_custom_key "$2" \
        --enable_analyzer 0

    $CLICKHOUSE_CLIENT \
        --query "$2" \
        --query_id "${1}_custom_key_analyzer" \
        --max_parallel_replicas 3 \
        --parallel_replicas_mode 'custom_key_sampling' \
        --parallel_replicas_custom_key "$2" \
        --enable_analyzer 1
}

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE replicated_numbers
    (
        number Int64,
    )
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/replicated_numbers', 'r1')
    ORDER BY (number)
    AS SELECT number FROM numbers(100000);
"

query_id_base="02783_count-$CLICKHOUSE_DATABASE"

run_query_with_pure_parallel_replicas "${query_id_base}_0" "SELECT count() FROM replicated_numbers"
run_query_with_pure_parallel_replicas "${query_id_base}_1" "SELECT * FROM (SELECT count() FROM replicated_numbers) LIMIT 20"

# Not implemented yet as the query fails to execute correctly to begin with
#run_query_with_custom_key_parallel_replicas "${query_id_base}_2" "SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), replicated_numbers)" "sipHash64(number)"
#run_query_with_custom_key_parallel_replicas "${query_id_base}_3" "SELECT * FROM (SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), replicated_numbers)) LIMIT 20" "sipHash64(number)"


$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
has_used_parallel_replicas "${query_id_base}"
