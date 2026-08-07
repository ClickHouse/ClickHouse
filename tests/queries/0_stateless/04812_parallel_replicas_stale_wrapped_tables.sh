#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would make every replica of every concurrent query look stale.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A replicated table can hide behind a wrapper storage: a view expands into its stored query at read
# time, and a materialized view forwards reads to its target table, so neither wrapper appears as a
# replicated table in the query fragment shipped to the replicas. The replicas execute the whole
# fragment, so a replica lagging on such a hidden replicated table must be excluded from coordinated
# reading as well when falling back to stale replicas is switched off.
#
# The `tables_status_report_replicated_tables_stale` failpoint makes the server report every
# *replicated* table as lagging in its `TablesStatus` responses while non-replicated tables (and the
# wrappers themselves) keep looking fresh - exactly the situation where the freshness check has to
# resolve the wrapper to the replicated table underneath.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT tables_status_report_replicated_tables_stale" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_dim_wrapped (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_dim_wrapped', '1') ORDER BY k;
    INSERT INTO t_pr_dim_wrapped SELECT number * 100 FROM numbers(10);
    INSERT INTO t_pr_dim_wrapped SELECT (number + 10) * 100 FROM numbers(10);
    CREATE VIEW v_pr_dim AS SELECT k FROM t_pr_dim_wrapped;
    CREATE MATERIALIZED VIEW mv_pr_dim ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mv_pr_dim', '1') ORDER BY k AS SELECT k FROM t_pr_dim_wrapped;
    INSERT INTO mv_pr_dim SELECT number * 100 FROM numbers(10);
    INSERT INTO mv_pr_dim SELECT (number + 10) * 100 FROM numbers(10);
    CREATE TABLE t_pr_mrgv_1 (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_pr_mrgv_2 (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_mrgv_1 SELECT number FROM numbers(1000);
    INSERT INTO t_pr_mrgv_2 SELECT number + 1000 FROM numbers(1000);
"

# `parallel_replicas_connect_timeout_ms` defaults to 300 ms, which a loaded CI machine overshoots,
# and a replica that missed the connection window does not count as available - the fresh case would
# then report 0 available replicas. `analyzer_inline_views` is pinned off so that the plain view
# reaches the freshness check as a wrapper storage and not as an already-inlined subquery.
PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_allow_materialized_views = 1, analyzer_inline_views = 0, max_replica_delay_for_distributed_queries = 1, fallback_to_stale_replicas_for_distributed_queries = 0, parallel_replicas_connect_timeout_ms = 30000"

# The remote connections are established lazily, on the first read from a remote source, so on a tiny
# table the initiator's local replica can finish the whole read and cancel the remotes before they are
# ever counted - the cases that assert participation keep the initiator out of the set of replicas
# that read, which makes the remote connections mandatory. The cases that assert exclusion keep the
# local replica in: it is the only one allowed to read there.
FRESH_SETTINGS="$PR_SETTINGS, parallel_replicas_prefer_local_replica = 0"

# Whether any replica was admitted to coordinated reading.
function replicas_used()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['ParallelReplicasAvailableCount'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'"
}

for source in "v_pr_dim" "mv_pr_dim" "t_pr_dim_wrapped"
do
    query="SELECT count(), sum(k) FROM $source"

    # All the replicas are up to date: reading is coordinated across them, as usual.
    query_id="04812_${CLICKHOUSE_DATABASE}_fresh_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query SETTINGS $FRESH_SETTINGS"
    replicas_used "$query_id"

    # Every replica lags behind on the replicated table (hidden behind the wrapper for the first two
    # sources): none of them may read, and the initiator reads everything itself, so the result is
    # still correct.
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT tables_status_report_replicated_tables_stale"

    query_id="04812_${CLICKHOUSE_DATABASE}_stale_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query SETTINGS $PR_SETTINGS"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT tables_status_report_replicated_tables_stale"

    replicas_used "$query_id"
done

# Control: with the same staleness, a query whose fragment reads nothing replicated still admits the
# replicas - the exclusion above comes from the replicated table, not from the failpoint as such.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT tables_status_report_replicated_tables_stale"

query_id="04812_${CLICKHOUSE_DATABASE}_control_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    SELECT count(), sum(k) FROM merge(currentDatabase(), '^t_pr_mrgv_') SETTINGS $FRESH_SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT tables_status_report_replicated_tables_stale"

replicas_used "$query_id"
