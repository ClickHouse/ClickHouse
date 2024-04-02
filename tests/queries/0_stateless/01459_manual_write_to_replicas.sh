#!/usr/bin/env bash
# Tags: replica, no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NUM_REPLICAS=10

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        DROP TABLE IF EXISTS r$i SYNC;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', 'r$i') ORDER BY x;
    "
done

function thread {
    for x in {0..99}; do
        # sometimes we can try to commit obsolete part if fetches will be quite fast,
        # so supress warning messages like "Tried to commit obsolete part ... covered by ..."
        # (2) keeper fault injection for inserts because
        #     it can be a cause of deduplicated parts be visible to SELECTs for sometime (until cleanup thread remove them),
        #     so the same SELECT on different replicas can return different results, i.e. test output will be non-deterministic
        #     (see #9712)
        $CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 --query "INSERT INTO r$1 SELECT $x % $NUM_REPLICAS = $1 ? $x - 1 : $x" 2>/dev/null  # Replace some records as duplicates so they will be written by other replicas
    done
}

for i in $(seq 1 $NUM_REPLICAS); do
    thread $i &
done

wait

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -n -q "
        SYSTEM SYNC REPLICA r$i;
        SELECT count(), min(x), max(x), sum(x) FROM r$i;"
done

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r$i SYNC;"
done
