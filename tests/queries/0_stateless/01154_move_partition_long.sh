#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

declare -A engines
engines[0]="MergeTree"
engines[1]="ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', toString(randConstant()))"
engines[2]="ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src_' || toString(randConstant()), 'single_replica')"

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE dst_$i (p UInt64, k UInt64, v UInt64)
          ENGINE=ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', '$i')
          PARTITION BY p % 10 ORDER BY k" 2>&1| grep -Pv "Retrying createReplica|created by another server at the same moment, will retry" &
    engine=${engines[$((i % ${#engines[@]}))]}
    $CLICKHOUSE_CLIENT -q "CREATE TABLE src_$i (p UInt64, k UInt64, v UInt64) ENGINE=$engine
          PARTITION BY p % 10 ORDER BY k" 2>&1| grep -Pv "Retrying createReplica|created by another server at the same moment, will retry" &
done
wait

#function create_drop_thread()
#{
#    while true; do
#        REPLICA=$(($RANDOM % 16))
#        $CLICKHOUSE_CLIENT -q "DROP TABLE src_$REPLICA;"
#        arr=("$@")
#        engine=${arr[$RANDOM % ${#arr[@]}]}
#        $CLICKHOUSE_CLIENT -q "CREATE TABLE src_$REPLICA (p UInt64, k UInt64, v UInt64) ENGINE=$engine PARTITION BY p % 10 ORDER BY k"
#        sleep 0.$RANDOM;
#    done
#}

function insert_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 16))
        LIMIT=$(($RANDOM % 100))
        $CLICKHOUSE_CLIENT -q "INSERT INTO $1_$REPLICA SELECT * FROM generateRandom('p UInt64, k UInt64, v UInt64') LIMIT $LIMIT" 2>/dev/null
    done
}

function move_partition_src_dst_thread()
{
    while true; do
        FROM_REPLICA=$(($RANDOM % 16))
        TO_REPLICA=$(($RANDOM % 16))
        PARTITION=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "ALTER TABLE src_$FROM_REPLICA MOVE PARTITION $PARTITION TO TABLE dst_$TO_REPLICA" 2>/dev/null
        sleep 0.$RANDOM;
    done
}

function replace_partition_src_src_thread()
{
    while true; do
        FROM_REPLICA=$(($RANDOM % 16))
        TO_REPLICA=$(($RANDOM % 16))
        PARTITION=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "ALTER TABLE src_$TO_REPLICA REPLACE PARTITION $PARTITION FROM src_$FROM_REPLICA" 2>/dev/null
        sleep 0.$RANDOM;
    done
}

function drop_partition_thread()
{
    while true; do
        REPLICA=$(($RANDOM % 16))
        PARTITION=$(($RANDOM % 10))
        $CLICKHOUSE_CLIENT -q "ALTER TABLE dst_$REPLICA DROP PARTITION $PARTITION" 2>/dev/null
        sleep 0.$RANDOM;
    done
}

function optimize_thread()
{
  while true; do
        REPLICA=$(($RANDOM % 16))
        TABLE="src"
        if (( RANDOM % 2 )); then
            TABLE="dst"
        fi
        $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${TABLE}_$REPLICA" 2>/dev/null
        sleep 0.$RANDOM;
    done
}

#export -f create_drop_thread;
export -f insert_thread;
export -f move_partition_src_dst_thread;
export -f replace_partition_src_src_thread;
export -f drop_partition_thread;
export -f optimize_thread;

TIMEOUT=60

#timeout $TIMEOUT bash -c "create_drop_thread ${engines[@]}" &
timeout $TIMEOUT bash -c 'insert_thread src' &
timeout $TIMEOUT bash -c 'insert_thread src' &
timeout $TIMEOUT bash -c 'insert_thread dst' &
timeout $TIMEOUT bash -c move_partition_src_dst_thread &
timeout $TIMEOUT bash -c replace_partition_src_src_thread &
timeout $TIMEOUT bash -c drop_partition_thread &
timeout $TIMEOUT bash -c optimize_thread &
wait

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA dst_$i" &
    $CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA src_$i" 2>/dev/null &
done
wait
echo "Replication did not hang"

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "DROP TABLE dst_$i" &
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS src_$i" &
done
wait
