#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `parallel_replicas_ship_join_predicate` left at 0, shipping an INNER JOIN's semi-join predicate into
# the replicas' fragment is decided by the automatic parallel replicas cost model: the first run measures
# the join's match rate, a later one ships the predicate only if the rows it removes outweigh the scan of
# the build side that builds the set.

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

PR_SETTINGS="enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0, automatic_parallel_replicas_min_bytes_per_replica = 0"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE sjpa_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple()
    AS SELECT number % 10000, rand() FROM numbers(2000000);

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
run_query sjpa_selective_dim 2 > /dev/null
echo "shipped: $(shipped_predicate "$(run_query sjpa_selective_dim 2)")"

echo 'selective join: the predicate is shipped once the match rate is known'
echo "shipped: $(shipped_predicate "$(run_query sjpa_selective_dim 1)")"

echo 'join that matches everything: the predicate is not worth shipping'
run_query sjpa_matching_dim 2 > /dev/null
echo "shipped: $(shipped_predicate "$(run_query sjpa_matching_dim 1)")"

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
