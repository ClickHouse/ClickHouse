#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every query that reads with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The initiator passes the child sets of the `Merge` tables it read to the replicas
# (`parallel_replicas_merge_child_tables`) when it builds no local plan, and a replica whose `Merge`
# table resolves to a different set fails the query instead of silently losing the rows of a child
# table that no participating replica announced.
#
# A query can read from more than one `Merge` table expression, and each of them must be compared
# against the set the initiator read for that same table expression: when the sets are accepted
# interchangeably, a leaf whose child set drifted into the child set of its sibling passes the check
# while reading tables the initiator planned for the other leaf, and the query silently returns
# wrong results.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_sibling_a1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    CREATE TABLE t_pr_merge_sibling_b1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    INSERT INTO t_pr_merge_sibling_a1 SELECT number, number FROM numbers(500);
    INSERT INTO t_pr_merge_sibling_b1 SELECT number, number * 10 FROM numbers(500);

"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"

# The left table expression matches both children, the right one only the second child, so dropping
# the first child makes the left one resolve to exactly the child set of the right one. Both leaves
# must be read by the replicas within one fragment, which is what a join of two `merge` table
# functions is offloaded as - and the two leaves are told apart only by their aliases, because a
# table function is identified by the name of the function.
QUERY="SELECT count(), sum(l.v), sum(r.v) FROM merge(currentDatabase(), '^t_pr_merge_sibling_(a1|b1)\$') AS l INNER JOIN merge(currentDatabase(), '^t_pr_merge_sibling_b1\$') AS r ON l.k = r.k"

# Sanity check: the query is eligible for parallel replicas and returns correct results.
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS"

# Pause the initiator after it has planned the query, and drop the child table that only the left
# `Merge` table matches, so that the left table resolves on the replicas to the child set the
# initiator read for the right table. Without comparing every table expression against its own set
# the query silently returns half of the rows.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT parallel_replicas_pause_before_sending_queries PAUSE"
# The drop must not be synchronous: the paused query holds a reference to the table, so waiting for
# the data to be finally dropped (the CI default, `database_atomic_wait_for_drop_and_detach_synchronously = 1`)
# would deadlock with the failpoint. Detaching the table from the catalog is immediate either way.
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP TABLE t_pr_merge_sibling_a1"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

wait $query_pid

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_pr_merge_sibling_b1;
"
