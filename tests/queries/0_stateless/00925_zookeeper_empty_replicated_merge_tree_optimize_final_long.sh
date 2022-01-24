#!/usr/bin/env bash
# Tags: long, replica, no-replicated-database
# Tag no-replicated-database: Old syntax is not allowed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS replicated_optimize1;"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS replicated_optimize2;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE replicated_optimize1 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00925/optimize', 'r1', d, k, 8192);"
$CLICKHOUSE_CLIENT -q "CREATE TABLE replicated_optimize2 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_00925/optimize', 'r2', d, k, 8192);"

num_tries=0
while [[ $($CLICKHOUSE_CLIENT -q "SELECT is_leader FROM system.replicas WHERE database=currentDatabase() AND table='replicated_optimize1'") -ne 1 ]]; do
    sleep 0.5;
    num_tries=$((num_tries-1))
    if [ $num_tries -eq 10 ]; then
        echo "Replica cannot become leader"
        break
    fi
done

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE replicated_optimize1 FINAL;"

$CLICKHOUSE_CLIENT -q "DROP TABLE replicated_optimize1;"
$CLICKHOUSE_CLIENT -q "DROP TABLE replicated_optimize2;"
