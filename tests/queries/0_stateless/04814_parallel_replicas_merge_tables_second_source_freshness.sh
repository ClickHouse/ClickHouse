#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoints are server-wide and would make every replica of every concurrent query look stale.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every storage created by a table function carries the same synthetic id (`_table_function.merge`),
# so the freshness walk over the query fragment must not deduplicate distinct `merge()` leaves by
# storage id: in `merge('^a_') JOIN merge('^b_')` the replicated children of the second leaf have to
# be checked too. Here one `merge()` source has only plain MergeTree children while the other one
# hides a replicated table, and the join is tried in both orders - whichever leaf the walk visits
# first, a replica lagging on the replicated child must be excluded from coordinated reading.
#
# The `tables_status_report_replicated_tables_stale` failpoint makes the server report every
# *replicated* table as lagging in its `TablesStatus` responses, and the plain MergeTree children
# keep looking fresh. The `slowdown_parallel_replicas_local_plan_read` failpoint keeps the
# initiator's local replica from finishing the whole tiny read before the remote replicas even
# establish their connections, which would leave `ParallelReplicasAvailableCount` at zero in the
# fresh case.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT tables_status_report_replicated_tables_stale" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_msf_plain (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_msf_plain SELECT number, number * 10 FROM numbers(1000);
    INSERT INTO t_pr_msf_plain SELECT number + 1000, number * 10 FROM numbers(1000);
    CREATE TABLE t_pr_msf_repl (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_msf_repl', '1') ORDER BY k;
    INSERT INTO t_pr_msf_repl SELECT number, number * 100 FROM numbers(1000);
    INSERT INTO t_pr_msf_repl SELECT number + 1000, number * 100 FROM numbers(1000);
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_replica_delay_for_distributed_queries = 1, fallback_to_stale_replicas_for_distributed_queries = 0, parallel_replicas_connect_timeout_ms = 30000"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read"

# Whether any replica was admitted to coordinated reading.
function replicas_used()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['ParallelReplicasAvailableCount'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'"
}

for join_order in "merge(currentDatabase(), '^t_pr_msf_plain$') AS l INNER JOIN merge(currentDatabase(), '^t_pr_msf_repl$') AS r" \
                  "merge(currentDatabase(), '^t_pr_msf_repl$') AS l INNER JOIN merge(currentDatabase(), '^t_pr_msf_plain$') AS r"
do
    query="SELECT count(), sum(l.v + r.v) FROM $join_order ON l.k = r.k SETTINGS $PR_SETTINGS"

    # All the replicas are up to date: reading is coordinated across them, as usual.
    # $$ keeps the query ids unique across the reruns of a flaky check: they reuse the database, `query_log` outlives a run, and $RANDOM alone collides between runs.
    query_id="04814_${CLICKHOUSE_DATABASE}_fresh_$$_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query"
    replicas_used "$query_id"

    # Every replica lags behind on the replicated table hidden in one of the two merge() sources:
    # none of them may read, and the initiator reads everything itself, so the result is still correct.
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT tables_status_report_replicated_tables_stale"

    query_id="04814_${CLICKHOUSE_DATABASE}_stale_$$_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT tables_status_report_replicated_tables_stale"

    replicas_used "$query_id"
done
