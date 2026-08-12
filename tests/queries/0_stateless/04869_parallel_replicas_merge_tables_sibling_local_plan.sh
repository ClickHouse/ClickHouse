#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is server-wide and would pause every query that reads with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The initiator passes the child sets of the `Merge` tables it planned to every node
# (`parallel_replicas_merge_child_tables`). A non-designated (sibling) `Merge` leaf of the query is
# read plainly and in full by every participating node, so a sibling whose child set drifts after
# the initiator planned the query would join different rows on different nodes - the query must
# fail closed instead, whether or not the initiator builds a local plan. The one exempt read is the
# coordinated leaf when the initiator pinned a snapshot replica (its local plan): the coordinator
# ignores streams the pinned replica did not announce and the pinned replica reads the ones nobody
# else announced in full, so a diverging set stays correct there and must not fail the query.

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT parallel_replicas_pause_before_sending_queries" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_merge_pause_before_reading" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_pr_merge_sib_lp_a1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    CREATE TABLE t_pr_merge_sib_lp_a2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    CREATE TABLE t_pr_merge_sib_lp_b1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    CREATE TABLE t_pr_merge_sib_lp_b2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
    INSERT INTO t_pr_merge_sib_lp_a1 SELECT number, number FROM numbers(500);
    INSERT INTO t_pr_merge_sib_lp_a2 SELECT 500 + number, 500 + number FROM numbers(500);
    INSERT INTO t_pr_merge_sib_lp_b1 SELECT number, number * 10 FROM numbers(500);
    INSERT INTO t_pr_merge_sib_lp_b2 SELECT 500 + number, (500 + number) * 10 FROM numbers(500);
"

PR_SETTINGS="enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_merge_tables = 1"
PINNED_SETTINGS="$PR_SETTINGS, parallel_replicas_prefer_local_replica = 1"
UNPINNED_SETTINGS="$PR_SETTINGS, parallel_replicas_prefer_local_replica = 0"

# The left `merge` table function is the leaf designated for coordinated reading, the right one is
# a plain-read sibling. A join of two `merge` table functions is offloaded as one fragment, so both
# leaves are read by every participating node.
QUERY="SELECT count(), sum(l.v), sum(r.v) FROM merge(currentDatabase(), '^t_pr_merge_sib_lp_a(1|2)\$') AS l INNER JOIN merge(currentDatabase(), '^t_pr_merge_sib_lp_b(1|2)\$') AS r ON l.k = r.k"

function recreate_b2()
{
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE t_pr_merge_sib_lp_b2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
        INSERT INTO t_pr_merge_sib_lp_b2 SELECT 500 + number, (500 + number) * 10 FROM numbers(500);
    "
}

# Runs the query with the given settings, dropping the given table while the query is paused on
# the given failpoint - after the initiator planned the query and recorded the child sets. Prints
# whatever the query prints: the error code on a failure, the result rows on a success.
#
# The failpoint matters: with a local plan and this little data the initiator finishes the whole
# read before the remote replicas even plan the query, so a pause before sending the queries to
# the replicas (`parallel_replicas_pause_before_sending_queries`) is checked by nobody - the local
# plan enumerated the children before the pause and the replicas are cancelled without planning.
# Pausing inside the `Merge` reads themselves (`storage_merge_pause_before_reading`) makes the
# initiator's own local plan enumerate the children after the drop. Without a local plan there is
# nothing to pause on the initiator, and the pause before sending the queries makes every replica
# plan after the drop.
function run_with_mid_pause_drop()
{
    local settings=$1
    local table_to_drop=$2
    local failpoint=$3

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $failpoint"

    $CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $settings" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED" || true &
    query_pid=$!

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $failpoint PAUSE"
    # The drop must not be synchronous: the paused query holds a reference to the table, so waiting
    # for the data to be finally dropped (the CI default,
    # `database_atomic_wait_for_drop_and_detach_synchronously = 1`) would deadlock with the
    # failpoint. Detaching the table from the catalog is immediate either way.
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP TABLE $table_to_drop"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint"

    wait $query_pid
}

# Sanity check: the query is eligible for parallel replicas and returns correct results.
echo "-- pinned, no drift"
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $PINNED_SETTINGS"

# The sibling leaf loses a child while the initiator is paused: the nodes would read the sibling
# through different child sets, so the query must fail closed - with and without a local plan.
echo "-- pinned, sibling drift"
run_with_mid_pause_drop "$PINNED_SETTINGS" "t_pr_merge_sib_lp_b2" "storage_merge_pause_before_reading"

recreate_b2
echo "-- unpinned, sibling drift"
run_with_mid_pause_drop "$UNPINNED_SETTINGS" "t_pr_merge_sib_lp_b2" "parallel_replicas_pause_before_sending_queries"

# The designated leaf loses a child while the initiator is paused, and the initiator pinned a
# snapshot replica: the coordinated read tolerates the divergence (the result is a consistent
# snapshot either way), so the query must succeed. The exact rows depend on when each node
# enumerates the children relative to the drop, so print only that it did not fail.
recreate_b2
echo "-- pinned, designated drift"
output=$(run_with_mid_pause_drop "$PINNED_SETTINGS" "t_pr_merge_sib_lp_a2" "storage_merge_pause_before_reading")
if [[ "$output" == *"SUPPORT_IS_DISABLED"* ]]; then
    echo "$output"
else
    echo "ok"
fi

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_pr_merge_sib_lp_a1;
    DROP TABLE t_pr_merge_sib_lp_b1;
    DROP TABLE t_pr_merge_sib_lp_b2;
"
