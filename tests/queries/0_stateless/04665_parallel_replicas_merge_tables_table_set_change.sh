#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause reading from Merge tables in concurrent tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Parallel replicas over a Merge table decide eligibility at planning time from one catalog walk,
# and enumerate the matching tables again at reading time. If a table that cannot be coordinated
# (not a MergeTree table) starts matching the regexp in between, the query must fail instead of
# silently reading that table in full on every replica and duplicating its rows in the result.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_merge_pause_before_reading" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_reval_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE t_pr_merge_reval_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_pr_merge_reval_1 SELECT number, number FROM numbers(1000);
    INSERT INTO t_pr_merge_reval_2 SELECT number + 1000, number FROM numbers(1000);
    CREATE TABLE t_pr_merge_reval ENGINE = Merge(currentDatabase(), '^t_pr_merge_reval_');
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"

# Sanity check: the query is eligible for parallel replicas and returns correct results.
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(k) FROM t_pr_merge_reval SETTINGS $PR_SETTINGS"

# Pause the query between planning and reading, and let a Log table matching the regexp
# appear in between.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_merge_pause_before_reading"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(k) FROM t_pr_merge_reval SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT storage_merge_pause_before_reading PAUSE"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_pr_merge_reval_3 (k UInt64, v UInt64) ENGINE = Log"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_merge_pause_before_reading"

wait $query_pid
