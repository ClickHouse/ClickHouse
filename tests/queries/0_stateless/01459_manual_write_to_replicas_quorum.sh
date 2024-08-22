#!/usr/bin/env bash
# Tags: replica, no-replicated-database, no-parallel, no-debug, no-random-settings
# Tag no-replicated-database: Fails due to additional replicas or shards

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test does many invocations of clickhouse-client in a loop,
# leading to "Too many parts" in the system.coverage_log,
# but we are not interested in client-side coverage here.
unset CLICKHOUSE_WRITE_COVERAGE

NUM_REPLICAS=10

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS r$i SYNC;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', 'r$i') ORDER BY x;
    "
done

valid_exceptions_to_retry='Quorum for previous write has not been satisfied yet|Another quorum insert has been already started|Unexpected logical error while adding block'

function thread {
    for x in {0..99}; do
        while true; do
            $CLICKHOUSE_CLIENT --insert_quorum 5 --insert_quorum_parallel 0 --insert_keeper_fault_injection_probability=0 --query "INSERT INTO r$1 SELECT $x" 2>&1 | grep -qE "$valid_exceptions_to_retry" || break
        done
    done
}

for i in $(seq 1 $NUM_REPLICAS); do
    thread $i &
done

wait

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "
        SYSTEM SYNC REPLICA r$i;
        SELECT count(), min(x), max(x), sum(x) FROM r$i;"
done

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r$i SYNC;"
done
