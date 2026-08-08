#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause reading from Merge tables in concurrent tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a replica lagging behind by more than `max_replica_delay_for_distributed_queries` must not
# participate in coordinated reading, the initiator checks the replication delay of the replicated
# tables underlying a Merge table - but only of those matching at planning time. A replicated table
# that starts matching between planning and reading was never checked, so a replica already admitted
# to coordinated reading may be lagging behind on it: the query must fail instead of serving data
# whose freshness nobody verified.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_merge_pause_before_reading" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_fsnap_1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_fsnap_1', '1') ORDER BY k;
    CREATE TABLE t_pr_merge_fsnap_2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_fsnap_2', '1') ORDER BY k;
    INSERT INTO t_pr_merge_fsnap_1 SELECT number FROM numbers(1000);
    INSERT INTO t_pr_merge_fsnap_2 SELECT number + 1000 FROM numbers(1000);
    CREATE TABLE t_pr_merge_fsnap ENGINE = Merge(currentDatabase(), '^t_pr_merge_fsnap_');
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"
STALE_GATE_SETTINGS="$PR_SETTINGS, max_replica_delay_for_distributed_queries = 1, fallback_to_stale_replicas_for_distributed_queries = 0"

# Sanity check: with the stale-replica gate active and a stable table set, the query works.
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(k) FROM t_pr_merge_fsnap SETTINGS $STALE_GATE_SETTINGS"

function run_paused_query()
{
    local settings=$1
    local table_to_create=$2

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_merge_pause_before_reading"

    (
        out=$($CLICKHOUSE_CLIENT --query "SELECT count(), sum(k) FROM t_pr_merge_fsnap SETTINGS $settings" 2>&1)
        echo "$out" | grep -o -m1 "SUPPORT_IS_DISABLED" || echo "$out"
    ) &
    local query_pid=$!

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT storage_merge_pause_before_reading PAUSE"
    $CLICKHOUSE_CLIENT --query "$table_to_create"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_merge_pause_before_reading"

    wait $query_pid
}

# A replicated table starts matching between planning and reading while the stale-replica gate is
# active: nobody checked its replication delay, so the query fails closed.
run_paused_query "$STALE_GATE_SETTINGS" "
    CREATE TABLE t_pr_merge_fsnap_3 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_fsnap_3', '1') ORDER BY k"

# A non-replicated MergeTree table appearing in the same window is fine: the freshness check does
# not apply to it (planning now sees t_pr_merge_fsnap_3, so it is in the checked snapshot).
run_paused_query "$STALE_GATE_SETTINGS" "
    CREATE TABLE t_pr_merge_fsnap_4 (k UInt64) ENGINE = MergeTree ORDER BY k"

# With falling back to stale replicas allowed (the default), no freshness is checked at all and a
# new replicated table is fine too: reading it is coordinated like any other MergeTree child.
run_paused_query "$PR_SETTINGS" "
    CREATE TABLE t_pr_merge_fsnap_5 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_fsnap_5', '1') ORDER BY k"
