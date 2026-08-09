#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every query that reads with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a query reads through a `Merge` table over replicated children, the connection-time status
# probe checks the underlying replicated tables (their replication delay is what matters for
# freshness) - but the wrapper itself still has to exist on a replica for it to plan the query.
# A replica that has the children but not the wrapper must be excluded from the pool as a soft
# miss (as a missing plain table always was), not admitted and failed with `UNKNOWN_TABLE` while
# planning. On this single-server localhost cluster dropping the wrapper removes it from every
# replica at once, so all of them are excluded and the query fails with
# `ALL_CONNECTION_TRIES_FAILED` - before the fix it failed with `UNKNOWN_TABLE` from an admitted
# replica.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_wrapper_1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_wrapper_1', 'r1') ORDER BY k;
    CREATE TABLE t_pr_merge_wrapper_2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_wrapper_2', 'r1') ORDER BY k;
    INSERT INTO t_pr_merge_wrapper_1 SELECT number FROM numbers(1000);
    INSERT INTO t_pr_merge_wrapper_2 SELECT number + 1000 FROM numbers(1000);
    CREATE TABLE t_pr_merge_wrapper ENGINE = Merge(currentDatabase(), '^t_pr_merge_wrapper_');
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"

QUERY="SELECT count(), sum(k) FROM t_pr_merge_wrapper"

# Sanity check: the query is eligible for parallel replicas and returns correct results.
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS"

# Pause the initiator after it has planned the query, and drop the wrapper in between: the
# replicas still have the (replicated) children the freshness probe checks, but can no longer
# plan a query that reads from the wrapper.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 -e "ALL_CONNECTION_TRIES_FAILED" -e "UNKNOWN_TABLE" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT parallel_replicas_pause_before_sending_queries PAUSE"
# The drop must not be synchronous: the paused query holds a reference to the table, so waiting
# for the data to be finally dropped (the CI default, `database_atomic_wait_for_drop_and_detach_synchronously = 1`)
# would deadlock with the failpoint. Detaching the table from the catalog is immediate either way.
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP TABLE t_pr_merge_wrapper"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

wait $query_pid

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_pr_merge_wrapper_1;
    DROP TABLE t_pr_merge_wrapper_2;
"
