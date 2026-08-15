#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every query that reads with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A shipped fragment can read a catalog `Merge` table both as the designated coordinated leaf and
# as a sibling. The connection-time `TablesStatus` probe must check every catalog wrapper: its
# replicated children can exist while the wrapper was dropped, in which case the replica must be
# excluded as a soft miss rather than fail later while planning the sibling.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_sibling_wrapper_a1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_sibling_wrapper_a1', 'r1') ORDER BY k;
    CREATE TABLE t_pr_merge_sibling_wrapper_a2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_sibling_wrapper_a2', 'r1') ORDER BY k;
    CREATE TABLE t_pr_merge_sibling_wrapper_b1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_sibling_wrapper_b1', 'r1') ORDER BY k;
    CREATE TABLE t_pr_merge_sibling_wrapper_b2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pr_merge_sibling_wrapper_b2', 'r1') ORDER BY k;
    INSERT INTO t_pr_merge_sibling_wrapper_a1 SELECT number FROM numbers(500);
    INSERT INTO t_pr_merge_sibling_wrapper_a2 SELECT 500 + number FROM numbers(500);
    INSERT INTO t_pr_merge_sibling_wrapper_b1 SELECT number FROM numbers(500);
    INSERT INTO t_pr_merge_sibling_wrapper_b2 SELECT 500 + number FROM numbers(500);
    CREATE TABLE t_pr_merge_sibling_wrapper_a (k UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_merge_sibling_wrapper_a[12]$');
    CREATE TABLE t_pr_merge_sibling_wrapper_b (k UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_merge_sibling_wrapper_b[12]$');
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"

# The left leaf is designated for coordinated reading; the right catalog `Merge` table is a
# plain-read sibling, so dropping only its wrapper exposes whether sibling wrappers were sent to
# the status probe.
QUERY="SELECT count(), sum(l.k), sum(r.k) FROM t_pr_merge_sibling_wrapper_a AS l INNER JOIN t_pr_merge_sibling_wrapper_b AS r ON l.k = r.k"

$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT parallel_replicas_pause_before_sending_queries"
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PR_SETTINGS" 2>&1 | grep -o -m1 -e "ALL_CONNECTION_TRIES_FAILED" -e "UNKNOWN_TABLE" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT parallel_replicas_pause_before_sending_queries PAUSE"
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP TABLE t_pr_merge_sibling_wrapper_b"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries"

wait $query_pid

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_pr_merge_sibling_wrapper_a;
    DROP TABLE t_pr_merge_sibling_wrapper_a1;
    DROP TABLE t_pr_merge_sibling_wrapper_a2;
    DROP TABLE t_pr_merge_sibling_wrapper_b1;
    DROP TABLE t_pr_merge_sibling_wrapper_b2;
"
