#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every query that reads with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The initiator designates one table expression of the query for coordinated reading, and every
# replica plans the same query again for itself. The eligibility of a `Merge` leaf depends on the set
# of its underlying tables, so a replica can designate a different leaf than the initiator did: here
# the `Merge` leaf becomes ineligible after the initiator has planned the query, while the joined
# `MergeTree` table stays eligible. A replica that coordinated reading from the joined table instead
# would read only its share of it, and the `Merge` table in full, duplicating rows in the result -
# the query must fail instead.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_join_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_pr_merge_join_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_merge_join_1 SELECT number, number FROM numbers(1000);
    INSERT INTO t_pr_merge_join_2 SELECT number + 1000, number FROM numbers(1000);
    CREATE TABLE t_pr_merge_join ENGINE = Merge(currentDatabase(), '^t_pr_merge_join_');

    CREATE TABLE t_pr_join_right (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_join_right SELECT number, number * 2 FROM numbers(2000);
"

# `parallel_replicas_prefer_local_replica = 0` keeps the initiator out of the set of replicas that read,
# so that every replica plans the query for itself instead of the initiator winning all the read tasks
# on such a small table and cancelling the others before they even start.
PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"

QUERY="SELECT count(), sum(m.k) FROM t_pr_merge_join AS m JOIN t_pr_join_right AS r ON m.k = r.k"

# Sanity check: the query is eligible for parallel replicas and returns correct results.
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS"

# Pause the initiator after it has planned the query and designated the `Merge` leaf, but before the
# replicas get the query and plan it themselves, and let a Log table matching the regexp appear in
# between, so that the replicas designate the joined table instead.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT parallel_replicas_pause_before_sending_queries PAUSE"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_pr_merge_join_3 (k UInt64, v UInt64) ENGINE = Log"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

wait $query_pid
