#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every parallel-replicas query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A catalog `Merge` storage is shared by table aliases. Each alias must retain its own child-set
# snapshot: serialized plan leaves are re-planned independently on participating replicas.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_repeated_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    CREATE TABLE t_pr_merge_repeated_b (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    CREATE TABLE t_pr_merge_repeated (k UInt64, v UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_merge_repeated_[ab]$');
    INSERT INTO t_pr_merge_repeated_a SELECT number, number FROM numbers(500);
    INSERT INTO t_pr_merge_repeated_b SELECT number, number * 10 FROM numbers(500);
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0, serialize_query_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"
QUERY="SELECT count(), sum(l.v), sum(r.v) FROM t_pr_merge_repeated AS l INNER JOIN t_pr_merge_repeated AS r ON l.k = r.k"

$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT parallel_replicas_pause_before_sending_queries"
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT parallel_replicas_pause_before_sending_queries PAUSE"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_pr_merge_repeated_a"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

wait $query_pid

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_pr_merge_repeated;
    DROP TABLE t_pr_merge_repeated_b;
"
