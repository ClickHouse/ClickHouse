#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every query that reads with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the initiator builds no local plan (`parallel_replicas_prefer_local_replica = 0`), the
# reading coordinator has no pinned snapshot replica: every underlying table of a `Merge` table is
# announced by whichever replicas matched it. A child table that the initiator matched but no
# participating replica did would never be announced at all, and its rows would silently vanish
# from the result. The initiator passes the child sets it saw to the replicas
# (`parallel_replicas_merge_child_tables`), and a replica whose `Merge` table resolves to a
# different set must fail the query instead.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_shrink_1 (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_pr_merge_shrink_2 (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_merge_shrink_1 SELECT number FROM numbers(1000);
    INSERT INTO t_pr_merge_shrink_2 SELECT number + 1000 FROM numbers(1000);
    CREATE TABLE t_pr_merge_shrink ENGINE = Merge(currentDatabase(), '^t_pr_merge_shrink_');
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"

QUERY="SELECT count(), sum(k) FROM t_pr_merge_shrink"

# Sanity check: the query is eligible for parallel replicas and returns correct results.
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS"

# Pause the initiator after it has planned the query, and drop one child table in between, so that
# the replicas resolve the `Merge` table to a narrower child set than the initiator did. Without
# the check the dropped child is simply announced by nobody and the query silently returns half of
# the rows.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT parallel_replicas_pause_before_sending_queries PAUSE"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_pr_merge_shrink_2"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

wait $query_pid

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_pr_merge_shrink;
    DROP TABLE t_pr_merge_shrink_1;
"
