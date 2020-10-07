#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

NUM_REPLICAS=10

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        DROP TABLE IF EXISTS r$i;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01509/parallel_quorum_many', 'r$i') ORDER BY x;
    "
done

$CLICKHOUSE_CLIENT -n -q "SYSTEM STOP REPLICATION QUEUES quorum2;"

function thread {
    for j in {0..9}; do
        a=$((($1 - 1) * 10 + $j))
        $CLICKHOUSE_CLIENT --insert_quorum 5 --insert_quorum_parallel 1 --query "INSERT INTO r$1 SELECT $a"
    done
}

for i in $(seq 1 $NUM_REPLICAS); do
    thread $i &
done

wait

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        SYSTEM SYNC REPLICA r$i;
        SELECT count(), min(x), max(x), sum(x) FROM r$i;
        DROP TABLE IF EXISTS r$i;
"
done
