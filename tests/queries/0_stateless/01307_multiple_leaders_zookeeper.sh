#!/usr/bin/env bash
# Tags: zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

NUM_REPLICAS=2
DATA_SIZE=200

SEQ=$(seq 0 $(($NUM_REPLICAS - 1)))

for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS r$REPLICA"; done
for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT --query "CREATE TABLE r$REPLICA (x UInt64) ENGINE = ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table', 'r$REPLICA') ORDER BY x SETTINGS min_bytes_for_wide_part = '10M';"; done

function thread()
{
    REPLICA=$1
    ITERATIONS=$2

    # It's legal to fetch something before insert finished
    $CLICKHOUSE_CLIENT --max_block_size 1 --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 --query "INSERT INTO r$REPLICA SELECT number * $NUM_REPLICAS + $REPLICA FROM numbers($ITERATIONS)" 2>&1 | grep -v -F "Tried to commit obsolete part"
}

for REPLICA in $SEQ; do
    thread "$REPLICA" $DATA_SIZE &
done

wait

for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA r$REPLICA"; done
for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM r$REPLICA"; done
for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT --query "DROP TABLE r$REPLICA"; done
