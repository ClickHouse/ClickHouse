#!/usr/bin/env bash
# Tags: no-parallel, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function run_with_custom_key {
    echo "query='$1' with custom_key='$2'"
    for prefer_localhost_replica in 0 1; do
        for filter_type in 'default' 'range'; do
            for max_replicas in {1..3}; do
                echo "filter_type='$filter_type' max_replicas=$max_replicas prefer_localhost_replica=$prefer_localhost_replica"
                query="$1 SETTINGS max_parallel_replicas=$max_replicas\
    , parallel_replicas_custom_key='$2'\
    , parallel_replicas_custom_key_filter_type='$filter_type'\
    , prefer_localhost_replica=$prefer_localhost_replica"
                $CLICKHOUSE_CLIENT --query="$query"
            done
        done
    done
}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS 02535_custom_key";

$CLICKHOUSE_CLIENT --query="CREATE TABLE 02535_custom_key (x String) ENGINE = MergeTree ORDER BY x";
$CLICKHOUSE_CLIENT --query="INSERT INTO 02535_custom_key VALUES ('Hello')";

run_with_custom_key "SELECT * FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02535_custom_key)" "sipHash64(x)"

$CLICKHOUSE_CLIENT --query="DROP TABLE 02535_custom_key"

$CLICKHOUSE_CLIENT --query="CREATE TABLE 02535_custom_key (x String, y UInt32) ENGINE = MergeTree ORDER BY cityHash64(x)"
$CLICKHOUSE_CLIENT --query="INSERT INTO 02535_custom_key SELECT toString(number), number % 3 FROM numbers(1000)"

function run_count_with_custom_key {
    run_with_custom_key "SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02535_custom_key) GROUP BY y ORDER BY y" "$1"
}

run_count_with_custom_key "y"
run_count_with_custom_key "cityHash64(y)"
run_count_with_custom_key "cityHash64(y) + 1"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 02535_custom_key) as t1 JOIN 02535_custom_key USING y" --allow_repeated_settings --parallel_replicas_custom_key="y" --send_logs_level="trace" 2>&1 | grep -Fac "JOINs are not supported with"

$CLICKHOUSE_CLIENT --query="DROP TABLE 02535_custom_key"
