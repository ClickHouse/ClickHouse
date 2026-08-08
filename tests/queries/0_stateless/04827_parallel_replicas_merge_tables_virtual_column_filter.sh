#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would make every replica of every concurrent query look stale.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query over a `Merge` table can prune the children it actually reads with the `_database` and
# `_table` virtual columns. The parallel replicas eligibility check and the freshness check of the
# underlying replicated tables are deliberately conservative and look at every table the `Merge`
# table matches, not only at the children the query selects: the checks run before the query plan
# (and with it the pushed-down filter that does the pruning) is built, and every replica has to
# arrive at the same decision. This test pins the behavior at the two points where the distinction
# shows:
#   - an unselected child that cannot be read with parallel replicas switches the feature off even
#     though it is never read - the query still returns correct (filtered) results;
#   - when every child is eligible, a `_table` filter narrows the read but keeps coordinated
#     reading, and the freshness gate (which checked the superset of children) admits the replicas.
# Deriving both checks from the filtered child set instead is tracked separately - these results
# would then flip from "not coordinated" to "coordinated".

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_vcf_good (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_vcf_good', '1') ORDER BY k;
    CREATE TABLE t_pr_merge_vcf_log (k UInt64) ENGINE = Log;
    INSERT INTO t_pr_merge_vcf_good SELECT number FROM numbers(1000);
    INSERT INTO t_pr_merge_vcf_log SELECT number + 1000 FROM numbers(1000);
    CREATE TABLE t_pr_merge_vcf ENGINE = Merge(currentDatabase(), '^t_pr_merge_vcf_');

    CREATE TABLE t_pr_merge_vcf_repl_1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_vcf_repl_1', '1') ORDER BY k;
    CREATE TABLE t_pr_merge_vcf_repl_2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_vcf_repl_2', '1') ORDER BY k;
    INSERT INTO t_pr_merge_vcf_repl_1 SELECT number FROM numbers(1000);
    INSERT INTO t_pr_merge_vcf_repl_2 SELECT number + 1000 FROM numbers(1000);
    CREATE TABLE t_pr_merge_vcf_repl ENGINE = Merge(currentDatabase(), '^t_pr_merge_vcf_repl_');
"

# `parallel_replicas_connect_timeout_ms` defaults to 300 ms, which a loaded CI machine overshoots.
PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1, parallel_replicas_connect_timeout_ms = 30000"

# The remote connections are established lazily, so on a tiny read the initiator's local replica can
# finish everything and cancel the remotes before they are ever counted - the cases that assert
# participation keep the initiator out of the set of replicas that read.
FRESH_SETTINGS="$PR_SETTINGS, parallel_replicas_prefer_local_replica = 0"

# The freshness gate is only active when stale replicas must not be read.
STALE_GATE_SETTINGS="$PR_SETTINGS, max_replica_delay_for_distributed_queries = 1, fallback_to_stale_replicas_for_distributed_queries = 0"

# Whether any replica was admitted to coordinated reading.
function replicas_used()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['ParallelReplicasAvailableCount'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'"
}

# An unselected `Log` child switches parallel replicas off for the whole `Merge` table even though
# the `_table` filter prunes it before reading: the result is correct, but not coordinated.
for source in "t_pr_merge_vcf" "merge(currentDatabase(), '^t_pr_merge_vcf_')"
do
    query_id="04827_${CLICKHOUSE_DATABASE}_pruned_log_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "
        SELECT count(), sum(k) FROM $source WHERE _table = 't_pr_merge_vcf_good'
        SETTINGS $FRESH_SETTINGS"
    replicas_used "$query_id"
done

# With every child eligible, the `_table` filter narrows the read but keeps coordinated reading,
# and the freshness snapshot taken over the superset of children admits the filtered read.
for settings in "$FRESH_SETTINGS" "$FRESH_SETTINGS, max_replica_delay_for_distributed_queries = 1, fallback_to_stale_replicas_for_distributed_queries = 0"
do
    for source in "t_pr_merge_vcf_repl" "merge(currentDatabase(), '^t_pr_merge_vcf_repl_')"
    do
        query_id="04827_${CLICKHOUSE_DATABASE}_filtered_$RANDOM"
        $CLICKHOUSE_CLIENT --query_id "$query_id" --query "
            SELECT count(), sum(k) FROM $source WHERE _table = 't_pr_merge_vcf_repl_1'
            SETTINGS $settings"
        replicas_used "$query_id"
    done
done

# A replica that lags behind on an unselected replicated child is excluded from coordinated reading
# of the filtered query as well (the freshness check covers every matched child): with every replica
# stale nobody participates, the initiator reads everything itself, and the result stays correct.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT replicated_merge_tree_all_replicas_stale"

for source in "t_pr_merge_vcf_repl" "merge(currentDatabase(), '^t_pr_merge_vcf_repl_')"
do
    query_id="04827_${CLICKHOUSE_DATABASE}_stale_$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "
        SELECT count(), sum(k) FROM $source WHERE _table = 't_pr_merge_vcf_repl_1'
        SETTINGS $STALE_GATE_SETTINGS"
    replicas_used "$query_id"
done

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale"
