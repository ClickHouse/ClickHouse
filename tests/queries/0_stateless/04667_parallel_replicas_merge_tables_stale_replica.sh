#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would make every replica of every concurrent query look stale.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Merge` table is never replicated itself, and the storage a `merge(...)` table function creates does
# not even exist on the replicas, so the status of the table the query names says nothing about the
# freshness of the data the query actually reads. Parallel replicas check the replication delay of the
# underlying replicated tables instead, so that a replica lagging behind more than
# `max_replica_delay_for_distributed_queries` does not participate in coordinated reading when falling
# back to stale replicas is switched off.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_stale_1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_stale_1', '1') ORDER BY k;
    CREATE TABLE t_pr_merge_stale_2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_stale_2', '1') ORDER BY k;
    INSERT INTO t_pr_merge_stale_1 SELECT number FROM numbers(1000);
    INSERT INTO t_pr_merge_stale_2 SELECT number + 1000 FROM numbers(1000);
    CREATE TABLE t_pr_merge_stale ENGINE = Merge(currentDatabase(), '^t_pr_merge_stale_');
    CREATE TABLE t_pr_dim_stale (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_dim_stale', '1') ORDER BY k;
    INSERT INTO t_pr_dim_stale SELECT number * 100 FROM numbers(20);
"

# `parallel_replicas_connect_timeout_ms` defaults to 300 ms, which a loaded CI machine overshoots,
# and a replica that missed the connection window does not count as available - the fresh case would
# then report 0 available replicas.
PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1, max_replica_delay_for_distributed_queries = 1, fallback_to_stale_replicas_for_distributed_queries = 0, parallel_replicas_connect_timeout_ms = 30000"

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

for source in "t_pr_merge_stale" "merge(currentDatabase(), '^t_pr_merge_stale_')"
do
    # All the replicas are up to date: reading is coordinated across them, as usual.
    # $$ keeps the query ids unique across the reruns of a flaky check: they reuse the database, `query_log` outlives a run, and $RANDOM alone collides between runs.
    query_id="04667_${CLICKHOUSE_DATABASE}_fresh_$$_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "SELECT count(), sum(k) FROM $source SETTINGS $FRESH_SETTINGS"
    replicas_used "$query_id"

    # Every replica lags behind on the underlying replicated tables: none of them may read, and the
    # initiator reads everything itself, so the result is still correct.
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT replicated_merge_tree_all_replicas_stale"

    query_id="04667_${CLICKHOUSE_DATABASE}_stale_$$_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "SELECT count(), sum(k) FROM $source SETTINGS $PR_SETTINGS"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale"

    replicas_used "$query_id"
done

# The same for the whole-stage path: a `JOIN` over a `Merge` source is offloaded to the replicas as a
# single query, and the freshness of the underlying replicated tables decides there as well.
for source in "t_pr_merge_stale" "merge(currentDatabase(), '^t_pr_merge_stale_')"
do
    query="SELECT count(), sum(m.k) FROM $source AS m INNER JOIN t_pr_dim_stale AS d ON m.k = d.k"

    query_id="04667_${CLICKHOUSE_DATABASE}_join_fresh_$$_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query SETTINGS $FRESH_SETTINGS"
    replicas_used "$query_id"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT replicated_merge_tree_all_replicas_stale"

    query_id="04667_${CLICKHOUSE_DATABASE}_join_stale_$$_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query SETTINGS $PR_SETTINGS"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale"

    replicas_used "$query_id"
done

# The replicas execute the whole shipped fragment, so a replica lagging on a replicated table that is
# only joined to the `Merge` source must be excluded as well, even when the `Merge` children themselves
# are not replicated and always look fresh. The merge-only control shows that with the same staleness
# the replicas are still admitted when nothing replicated is read.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_plain_1 (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_pr_merge_plain_2 (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_merge_plain_1 SELECT number FROM numbers(1000);
    INSERT INTO t_pr_merge_plain_2 SELECT number + 1000 FROM numbers(1000);
"

PLAIN_SETTINGS="$PR_SETTINGS, parallel_replicas_for_non_replicated_merge_tree = 1"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT replicated_merge_tree_all_replicas_stale"

query_id="04667_${CLICKHOUSE_DATABASE}_control_$$_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    SELECT count(), sum(k) FROM merge(currentDatabase(), '^t_pr_merge_plain_')
    SETTINGS $PLAIN_SETTINGS, parallel_replicas_prefer_local_replica = 0"

query_id_join="04667_${CLICKHOUSE_DATABASE}_joined_dim_stale_$$_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id_join" --query "
    SELECT count(), sum(m.k) FROM merge(currentDatabase(), '^t_pr_merge_plain_') AS m
    INNER JOIN t_pr_dim_stale AS d ON m.k = d.k SETTINGS $PLAIN_SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale"

replicas_used "$query_id"
replicas_used "$query_id_join"
