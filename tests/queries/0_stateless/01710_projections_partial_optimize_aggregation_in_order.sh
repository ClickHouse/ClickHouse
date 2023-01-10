#!/usr/bin/env bash

# Test for optimize_aggregation_in_order with partial projections, i.e.:
# - first part will not have projection
# - second part will have projection
# And so two different aggregation should be done.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE IF EXISTS in_order_agg_partial_01710;

    CREATE TABLE in_order_agg_partial_01710
    (
        k1 UInt32,
        k2 UInt32,
        k3 UInt32,
        value UInt32
    )
    ENGINE = MergeTree
    ORDER BY tuple();

    INSERT INTO in_order_agg_partial_01710 SELECT 1, number%2, number%4, number FROM numbers(50000);
    SYSTEM STOP MERGES in_order_agg_partial_01710;
    ALTER TABLE in_order_agg_partial_01710 ADD PROJECTION aaaa (
        SELECT
            k1,
            k2,
            k3,
            sum(value)
        GROUP BY k1, k2, k3
    );
    INSERT INTO in_order_agg_partial_01710 SELECT 1, number%2, number%4, number FROM numbers(100000) LIMIT 50000, 100000;
"

function run_query()
{
    local query=$1 && shift

    local query_id
    query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-$(random_str 6)"

    echo "$query"
    local opts=(
        --allow_experimental_projection_optimization 1
        --force_optimize_projection 1
        --log_processors_profiles 1
        --query_id "$query_id"
    )
    $CLICKHOUSE_CLIENT "${opts[@]}" "$@" -q "$query"

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

    echo "Used processors:"
    $CLICKHOUSE_CLIENT --param_query_id "$query_id" -q "SELECT DISTINCT name FROM system.processors_profile_log WHERE query_id = {query_id:String} AND name LIKE 'Aggregating%'"
}

run_query "SELECT k1, k2, k3, sum(value) v FROM in_order_agg_partial_01710 GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order=0"
run_query "SELECT k1, k2, k3, sum(value) v FROM in_order_agg_partial_01710 GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order=1"
run_query "SELECT k1, k3, sum(value) v FROM in_order_agg_partial_01710 GROUP BY k1, k3 ORDER BY k1, k3 SETTINGS optimize_aggregation_in_order=0"
run_query "SELECT k1, k3, sum(value) v FROM in_order_agg_partial_01710 GROUP BY k1, k3 ORDER BY k1, k3 SETTINGS optimize_aggregation_in_order=1"
