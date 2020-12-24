#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

NUM_REPLICAS=10

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        DROP TABLE IF EXISTS r$i;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01459_manual_write_ro_replicas_quorum/r', 'r$i') ORDER BY x;
    "
done

function thread {
    for x in {0..99}; do
        while true; do
            $CLICKHOUSE_CLIENT --insert_quorum 5 --query "INSERT INTO r$1 SELECT $x" 2>&1 | grep -qF 'Quorum for previous write has not been satisfied yet' || break
        done
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
