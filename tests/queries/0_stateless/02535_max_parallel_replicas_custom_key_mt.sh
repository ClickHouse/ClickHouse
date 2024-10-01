#!/usr/bin/env bash
# Tags: no-parallel, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function run_with_custom_key {
    echo "query='$1' with custom_key='$2'"
    for filter_type in 'custom_key_sampling' 'custom_key_range'; do
        for max_replicas in {1..3}; do
            echo "filter_type='$filter_type' max_replicas=$max_replicas"
            query="$1 SETTINGS max_parallel_replicas=$max_replicas\
, enable_parallel_replicas='1' \
, parallel_replicas_mode ='$filter_type'\
, parallel_replicas_custom_key='$2'\
, parallel_replicas_for_non_replicated_merge_tree=1 \
, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost'"
            $CLICKHOUSE_CLIENT --query="$query"
        done
    done
}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS 02535_custom_key_mt";

$CLICKHOUSE_CLIENT --query="CREATE TABLE 02535_custom_key_mt (x String) ENGINE = MergeTree ORDER BY x";
$CLICKHOUSE_CLIENT --query="INSERT INTO 02535_custom_key_mt VALUES ('Hello')";

run_with_custom_key "SELECT * FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02535_custom_key_mt)" "sipHash64(x)"
run_with_custom_key "SELECT * FROM 02535_custom_key_mt" "sipHash64(x)"

$CLICKHOUSE_CLIENT --query="DROP TABLE 02535_custom_key_mt"

$CLICKHOUSE_CLIENT --query="CREATE TABLE 02535_custom_key_mt (x String, y UInt32) ENGINE = MergeTree ORDER BY cityHash64(x)"
$CLICKHOUSE_CLIENT --query="INSERT INTO 02535_custom_key_mt SELECT toString(number), number % 3 FROM numbers(1000)"

function run_count_with_custom_key_distributed {
    run_with_custom_key "SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02535_custom_key_mt) GROUP BY y ORDER BY y" "$1"
}

run_count_with_custom_key_distributed "y"
run_count_with_custom_key_distributed "cityHash64(y)"
run_count_with_custom_key_distributed "cityHash64(y) + 1"

function run_count_with_custom_key_merge_tree {
    run_with_custom_key "SELECT y, count() FROM 02535_custom_key_mt GROUP BY y ORDER BY y" "$1"
}

run_count_with_custom_key_merge_tree "y"
run_count_with_custom_key_merge_tree "cityHash64(y)"
run_count_with_custom_key_merge_tree "cityHash64(y) + 1"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02535_custom_key_mt) as t1 JOIN 02535_custom_key_mt USING y" --allow_repeated_settings --parallel_replicas_custom_key="y" --send_logs_level="trace" 2>&1 | grep -Fac "JOINs are not supported with"

$CLICKHOUSE_CLIENT --query="DROP TABLE 02535_custom_key_mt"
