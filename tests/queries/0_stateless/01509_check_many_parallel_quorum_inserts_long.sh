#!/usr/bin/env bash
# Tags: long, no-replicated-database
# Tag no-replicated-database: Fails due to additional replicas or shards

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NUM_REPLICAS=6

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        DROP TABLE IF EXISTS r$i;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/parallel_quorum_many', 'r$i') ORDER BY x;
    "
done

function thread {
    i=0 retries=300
    while [[ $i -lt $retries ]]; do # server can be dead
        $CLICKHOUSE_CLIENT --insert_quorum 3 --insert_quorum_parallel 1 --query "INSERT INTO r$1 SELECT $2" && break
        ((++i))
        sleep 0.1
    done
}

for i in $(seq 1 $NUM_REPLICAS); do
    for j in {0..4}; do
        a=$((($i - 1) * 10 + $j))

        # Note: making 30 connections simultaneously is a mini-DoS when server is build with sanitizers and CI environment is overloaded.
        # That's why we repeat "socket timeout" errors.
        thread $i $a 2>&1 | grep -v -P 'SOCKET_TIMEOUT|NETWORK_ERROR|^$' &
    done
done

wait

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        SYSTEM SYNC REPLICA r$i;
        SELECT count(), min(x), max(x), sum(x) FROM r$i;
    "
done

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "DROP TABLE IF EXISTS r$i SYNC;"
done
