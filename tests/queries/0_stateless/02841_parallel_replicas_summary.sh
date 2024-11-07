#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function involved_parallel_replicas () {
    # Not using current_database = '$CLICKHOUSE_DATABASE' as nested parallel queries aren't run with it
    $CLICKHOUSE_CLIENT --query "
        SELECT
            initial_query_id,
            countIf(initial_query_id != query_id) != 0  as parallel_replicas_were_used
        FROM system.query_log
    WHERE event_date >= yesterday()
      AND initial_query_id LIKE '$1%'
    GROUP BY initial_query_id
    ORDER BY min(event_time_microseconds) ASC
    FORMAT TSV"
}

$CLICKHOUSE_CLIENT --query "CREATE TABLE replicas_summary (n Int64) ENGINE = MergeTree() ORDER BY n AS Select * from numbers(100_000)"

# Note that we are not verifying the exact read rows and bytes (apart from not being 0) for 2 reasons:
# - Different block sizes lead to different read rows
# - Depending on how fast the replicas are they might need data that ends up being discarded because the coordinator
# already has enough (but it has been read in parallel, so it's reported).

query_id_base="02841_summary_$CLICKHOUSE_DATABASE"

# TODO: rethink the test, for now temporary disable parallel_replicas_local_plan
echo "
    SELECT *
    FROM replicas_summary
    LIMIT 100
    SETTINGS
        max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        enable_parallel_replicas = 2,
        parallel_replicas_for_non_replicated_merge_tree = 1,
        interactive_delay=0,
        parallel_replicas_local_plan=0
    "\
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&query_id=${query_id_base}_interactive_0" --data-binary @- -vvv 2>&1 \
    | grep "Summary" | grep -cv '"read_rows":"0"'

echo "
    SELECT *
    FROM replicas_summary
    LIMIT 100
    SETTINGS
        max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        enable_parallel_replicas = 2,
        parallel_replicas_for_non_replicated_merge_tree = 1,
        interactive_delay=99999999999,
        parallel_replicas_local_plan=0
    "\
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&query_id=${query_id_base}_interactive_high" --data-binary @- -vvv 2>&1 \
    | grep "Summary" | grep -cv '"read_rows":"0"'

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
involved_parallel_replicas "${query_id_base}"
