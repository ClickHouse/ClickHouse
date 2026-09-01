#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `parallel_replicas_ship_join_predicate` left at 0, shipping an INNER JOIN's semi-join predicate into
# the replicas' fragment is decided by the automatic parallel replicas cost model: a first run measures the
# join's match rate, a later one ships the predicate only if the rows it removes outweigh the scan of the
# build side that builds the set.
#
# `max_threads` is pinned low, and `merge_tree_min_bytes_per_task_for_remote_reading` with it, so that
# reading this much data on one node is worth splitting across replicas at all: the cost model caps the
# reading threads it credits either plan with at `input_bytes / merge_tree_min_bytes_per_task_for_remote_reading`,
# and a large enough value collapses both sides of the comparison and declines parallel replicas outright,
# never reaching the shipping decision.
# The join's orientation is pinned because the match rate priced here is the one of the join's probe side:
# with the fragment moved to the build side there is no measured rate for it and nothing is shipped.

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=debug"}

PR_SETTINGS="enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0, automatic_parallel_replicas_min_bytes_per_replica = 0,
    max_threads = 2,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    merge_tree_min_bytes_per_task_for_remote_reading = 1"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE sjpa_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    AS SELECT number % 10000, cityHash64(number) FROM numbers(2000000);

    -- 10 of the probe's 10000 keys: a match rate of 0.001, so the predicate removes almost everything.
    CREATE TABLE sjpa_selective_dim (k UInt64) ENGINE = MergeTree ORDER BY k
    AS SELECT number FROM numbers(10);

    -- Every key of the probe: the predicate removes nothing and only costs a scan to build the set.
    CREATE TABLE sjpa_matching_dim (k UInt64) ENGINE = MergeTree ORDER BY k
    AS SELECT number FROM numbers(10000);
"

# $1 -> dimension table, $2 -> automatic_parallel_replicas_mode (2 only collects statistics, 1 also applies)
function run_query () {
    $CLICKHOUSE_CLIENT_TRACE -q "
        SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpa_probe GROUP BY k) AS agg
        JOIN $1 AS d ON agg.k = d.k
        FORMAT Null
        SETTINGS $PR_SETTINGS, automatic_parallel_replicas_mode = $2" 2>&1
}

function shipped_predicate () {
    if grep -q "Shipping the join predicate into the replicas' fragment" <<< "$1"; then echo 1; else echo 0; fi
}

echo 'selective join: nothing is shipped on the run that measures it'
echo "shipped: $(shipped_predicate "$(run_query sjpa_selective_dim 2)")"

echo 'selective join: the predicate is shipped once the match rate is known'
echo "shipped: $(shipped_predicate "$(run_query sjpa_selective_dim 1)")"

echo 'join that matches everything: the predicate is not worth shipping'
run_query sjpa_matching_dim 2 > /dev/null
echo "shipped: $(shipped_predicate "$(run_query sjpa_matching_dim 1)")"

# Shipping the predicate is worth nothing unless it reaches the read as a key condition, which is a separate
# step from injecting it - the plan with parallel replicas normally reuses the single-node index analysis,
# made before the predicate existed. Assert the pruning, not just the decision.
echo 'shipping prunes what the replicas read'
$CLICKHOUSE_CLIENT -q "
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpa_probe GROUP BY k) AS agg
    JOIN sjpa_selective_dim AS d ON agg.k = d.k
    FORMAT Null
    SETTINGS $PR_SETTINGS, automatic_parallel_replicas_mode = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_noship';

    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpa_probe GROUP BY k) AS agg
    JOIN sjpa_selective_dim AS d ON agg.k = d.k
    FORMAT Null
    SETTINGS $PR_SETTINGS, automatic_parallel_replicas_mode = 1, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_ship';
"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT concat('shipped reads 10x fewer rows: ', toString(
        10 * anyIf(read_rows, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_ship')
            < anyIf(read_rows, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_noship')))
    FROM system.query_log
    WHERE log_comment IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_ship', '${CLICKHOUSE_TEST_UNIQUE_NAME}_noship')
      AND type = 'QueryFinish' AND query_id = initial_query_id AND event_date >= yesterday()
"

echo 'the result does not depend on the decision'
$CLICKHOUSE_CLIENT -q "
    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpa_probe GROUP BY k) AS agg
    JOIN sjpa_selective_dim AS d ON agg.k = d.k
    SETTINGS $PR_SETTINGS, automatic_parallel_replicas_mode = 1;

    SELECT sum(agg.s) FROM (SELECT k, sum(v) AS s FROM sjpa_probe GROUP BY k) AS agg
    JOIN sjpa_selective_dim AS d ON agg.k = d.k
    SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE sjpa_probe;
    DROP TABLE sjpa_selective_dim;
    DROP TABLE sjpa_matching_dim;
"
