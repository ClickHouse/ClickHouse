#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

NUM_REPLICAS=2
DATA_SIZE=200

SEQ=$(seq 0 $(($NUM_REPLICAS - 1)))

for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT -n --query "DROP TABLE IF EXISTS r$REPLICA"; done
for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT -n --query "CREATE TABLE r$REPLICA (x UInt64) ENGINE = ReplicatedMergeTree('/test/table', 'r$REPLICA') ORDER BY x SETTINGS min_bytes_for_wide_part = '10M';"; done

function thread()
{
    REPLICA=$1
    ITERATIONS=$2

    $CLICKHOUSE_CLIENT --max_block_size 1 --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 --query "INSERT INTO r$REPLICA SELECT number * $NUM_REPLICAS + $REPLICA FROM numbers($ITERATIONS)"
}

for REPLICA in $SEQ; do
    thread "$REPLICA" $DATA_SIZE &
done

wait

for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT -n --query "SYSTEM SYNC REPLICA r$REPLICA"; done
for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT -n --query "SELECT count(), sum(x) FROM r$REPLICA"; done
for REPLICA in $SEQ; do $CLICKHOUSE_CLIENT -n --query "DROP TABLE r$REPLICA"; done
