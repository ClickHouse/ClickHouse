#!/usr/bin/env bash
# Tags: long, no-replicated-database
# Tag no-replicated-database: Fails due to additional replicas or shards

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NUM_REPLICAS=2
NUM_INSERTS=5

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS r$i;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/parallel_quorum', 'r$i') ORDER BY x;
    "
done

$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES r2;"

function thread {
    $CLICKHOUSE_CLIENT --insert_quorum 2 --insert_quorum_parallel 1 --query "INSERT INTO r1 SELECT $1"
}

for i in $(seq 1 $NUM_INSERTS); do
    thread $i &
done

$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES r2;"

wait

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "
        SELECT count(), min(x), max(x), sum(x) FROM r$i;
        DROP TABLE IF EXISTS r$i;
"
done
