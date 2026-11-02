#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The total number of rows to read must be reported in progress for queries with parallel replicas,
# including the default case when the initiator uses the local plan.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pr_progress;
    CREATE TABLE t_pr_progress (x UInt64, s String) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192;
    INSERT INTO t_pr_progress SELECT number, toString(number) FROM numbers(1000000);
"

PR_SETTINGS="enable_parallel_replicas=1&max_parallel_replicas=3&cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost&parallel_replicas_for_non_replicated_merge_tree=1&automatic_parallel_replicas_mode=0"

function summary()
{
    $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&$1" -d "$2" -v 2>&1 \
        | grep -F "X-ClickHouse-Summary" | grep -oE '"(read_rows|total_rows_to_read)":"[0-9]+"' | paste -sd ' '
}

for settings in \
    "enable_parallel_replicas=0" \
    "$PR_SETTINGS&parallel_replicas_local_plan=0" \
    "$PR_SETTINGS&parallel_replicas_local_plan=1"
do
    echo "${settings##*&}"
    summary "$settings" "SELECT count() FROM t_pr_progress WHERE NOT ignore(s) FORMAT Null"
    summary "$settings&optimize_read_in_order=1" "SELECT x FROM t_pr_progress WHERE NOT ignore(s) ORDER BY x FORMAT Null"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_pr_progress"
