#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every parallel-replicas query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A non-inlined view is expanded only while each replica executes the shipped fragment. Its
# `Merge` input must therefore contribute a child-set snapshot even with the stale-replica gate
# disabled; otherwise a table dropped after planning lets the replicas read different view rows.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_view_child_set_driver (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_pr_merge_view_child_set_a (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_pr_merge_view_child_set_b (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_merge_view_child_set_driver SELECT number FROM numbers(500);
    INSERT INTO t_pr_merge_view_child_set_a SELECT number FROM numbers(500);
    INSERT INTO t_pr_merge_view_child_set_b SELECT 500 + number FROM numbers(500);
    CREATE TABLE t_pr_merge_view_child_set (k UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_merge_view_child_set_[ab]$');
    CREATE VIEW t_pr_merge_view_child_set_view AS SELECT k FROM t_pr_merge_view_child_set;
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1, parallel_replicas_allow_view_over_mergetree = 1, analyzer_inline_views = 0"
QUERY="SELECT count() FROM t_pr_merge_view_child_set_driver AS d INNER JOIN t_pr_merge_view_child_set_view AS v ON d.k = v.k"

$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT parallel_replicas_pause_before_sending_queries"
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT parallel_replicas_pause_before_sending_queries PAUSE"
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP TABLE t_pr_merge_view_child_set_a"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

wait $query_pid

$CLICKHOUSE_CLIENT --query "
    DROP VIEW t_pr_merge_view_child_set_view;
    DROP TABLE t_pr_merge_view_child_set;
    DROP TABLE t_pr_merge_view_child_set_driver;
    DROP TABLE t_pr_merge_view_child_set_b;
"
