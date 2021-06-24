#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE tbl_$i (p UInt64, k UInt64, v UInt64)
          ENGINE=ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/tbl', '$i')
          PARTITION BY p % 10 ORDER BY k" 2>&1| grep -Pv "Retrying createReplica|created by another server at the same moment, will retry" &
done
wait

function insert_thread()
{
    while true; do
        REPLICA=$((RANDOM % 16))
        LIMIT=$((RANDOM % 100))
        $CLICKHOUSE_CLIENT -q "INSERT INTO tbl_$REPLICA SELECT * FROM generateRandom('p UInt64, k UInt64, v UInt64') LIMIT $LIMIT" 2>/dev/null
    done
}

function replace_partition_update_thread()
{
    while true; do
        REPLICA=$((RANDOM % 16))
        PARTITION=$((RANDOM % 10))
        SRC_PARTITION=$((RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "ALTER TABLE tbl_$REPLICA REPLACE PARTITION $PARTITION FROM PARTITION $SRC_PARTITION UPDATE p = $PARTITION" 2>/dev/null
        sleep 0.$RANDOM;
    done
}

function update_thread()
{
    while true; do
        REPLICA=$((RANDOM % 16))
        PARTITION=$((RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "ALTER TABLE tbl_$REPLICA UPDATE v = v + 1 IN PARTITION $PARTITION WHERE 1" 2>/dev/null
        sleep 0.$RANDOM;
    done
}

function optimize_thread()
{
  while true; do
        REPLICA=$((RANDOM % 16))
        $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE tbl_$REPLICA" 2>/dev/null
        sleep 0.$RANDOM;
    done
}

export -f insert_thread;
export -f replace_partition_update_thread;
export -f update_thread;
export -f optimize_thread;

TIMEOUT=60

timeout $TIMEOUT bash -c insert_thread &
timeout $TIMEOUT bash -c replace_partition_update_thread &
timeout $TIMEOUT bash -c update_thread &
timeout $TIMEOUT bash -c optimize_thread &
wait

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA tbl_$i" 2>/dev/null &
done
wait
echo "Replication did not hang"

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "DROP TABLE tbl_$i" &
done
wait
