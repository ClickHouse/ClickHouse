#!/usr/bin/env bash
# Tags: replica, no-replicated-database, no-parallel
# Tag no-replicated-database: Fails due to additional replicas or shards

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NUM_REPLICAS=6

for i in $(seq 1 $NUM_REPLICAS); do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS r$i SYNC;
        CREATE TABLE r$i (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', 'r$i') ORDER BY x;
    "
done

valid_exceptions_to_retry='Quorum for previous write has not been satisfied yet|Another quorum insert has been already started|Unexpected logical error while adding block'

function thread {
    for x in {0..9}; do
        while true; do
            $CLICKHOUSE_CLIENT --query "DETACH TABLE r$1"
            $CLICKHOUSE_CLIENT --query "ATTACH TABLE r$1"
            $CLICKHOUSE_CLIENT --insert_quorum 3 --insert_quorum_parallel 0 --query "INSERT INTO r$1 SELECT $x" 2>&1 | grep -qE "$valid_exceptions_to_retry" || break
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
    # We filter out 'Removing temporary directory' on table DROP because in this test
    # we constantly DETACH and ATTACH tables. So some replica can start fetching some part
    # and other replica can be DETACHed during fetch. We will get unfinished tmp directory
    # which should be removed in background, but it's async operation so the tmp directory can
    # left on disk until table DROP.
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r$i SYNC;" 2>&1 | grep -v 'Removing temporary directory' ||:
done
