#!/usr/bin/env bash
# Tags: long, no-parallel, no-s3-storage
# FIXME: s3 storage should work OK, it
# reproduces bug which exists not only in S3 version.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

declare -A engines
engines[0]="MergeTree"
engines[1]="ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{shard}/src', '{replica}_' || toString(randConstant()))"
engines[2]="ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{shard}/src_' || toString(randConstant()), '{replica}')"

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE dst_$i (p UInt64, k UInt64, v UInt64)
          ENGINE=ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', '$i')
          PARTITION BY p % 10 ORDER BY k" 2>&1| grep -Pv "Retrying createReplica|created by another server at the same moment, will retry|is already started to be removing" 2>&1 &
    engine=${engines[$((i % ${#engines[@]}))]}
    $CLICKHOUSE_CLIENT -q "CREATE TABLE src_$i (p UInt64, k UInt64, v UInt64) ENGINE=$engine
          PARTITION BY p % 10 ORDER BY k" 2>&1| grep -Pv "Retrying createReplica|created by another server at the same moment, will retry|is already started to be removing" 2>&1 &
done
wait

#function create_drop_thread()
#{
#    REPLICA=$(($RANDOM % 16))
#    $CLICKHOUSE_CLIENT -q "DROP TABLE src_$REPLICA;"
#    arr=("$@")
#    engine=${arr[$RANDOM % ${#arr[@]}]}
#    $CLICKHOUSE_CLIENT -q "CREATE TABLE src_$REPLICA (p UInt64, k UInt64, v UInt64) ENGINE=$engine PARTITION BY p % 10 ORDER BY k"
#    sleep 0.$RANDOM
#}

function insert_thread()
{
    REPLICA=$(($RANDOM % 16))
    LIMIT=$(($RANDOM % 100))
    $CLICKHOUSE_CLIENT -q "INSERT INTO $1_$REPLICA SELECT * FROM generateRandom('p UInt64, k UInt64, v UInt64') LIMIT $LIMIT" 2>/dev/null
}

function move_partition_src_dst_thread()
{
    FROM_REPLICA=$(($RANDOM % 16))
    TO_REPLICA=$(($RANDOM % 16))
    PARTITION=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -q "ALTER TABLE src_$FROM_REPLICA MOVE PARTITION $PARTITION TO TABLE dst_$TO_REPLICA" 2>/dev/null
    sleep 0.$RANDOM
}

function replace_partition_src_src_thread()
{
    FROM_REPLICA=$(($RANDOM % 16))
    TO_REPLICA=$(($RANDOM % 16))
    PARTITION=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -q "ALTER TABLE src_$TO_REPLICA REPLACE PARTITION $PARTITION FROM src_$FROM_REPLICA" 2>/dev/null
    sleep 0.$RANDOM
}

function drop_partition_thread()
{
    REPLICA=$(($RANDOM % 16))
    PARTITION=$(($RANDOM % 10))
    $CLICKHOUSE_CLIENT -q "ALTER TABLE dst_$REPLICA DROP PARTITION $PARTITION" 2>/dev/null
    sleep 0.$RANDOM
}

function optimize_thread()
{
    REPLICA=$(($RANDOM % 16))
    TABLE="src"
    if (( RANDOM % 2 )); then
        TABLE="dst"
    fi
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${TABLE}_$REPLICA" 2>/dev/null
    sleep 0.$RANDOM
}

function drop_part_thread()
{
    REPLICA=$(($RANDOM % 16))
    part=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE active AND database='$CLICKHOUSE_DATABASE' and table='dst_$REPLICA' ORDER BY rand() LIMIT 1")
    $CLICKHOUSE_CLIENT -q "ALTER TABLE dst_$REPLICA DROP PART '$part'" 2>/dev/null
    sleep 0.$RANDOM
}

#export -f create_drop_thread;
export -f insert_thread
export -f move_partition_src_dst_thread
export -f replace_partition_src_src_thread
export -f drop_partition_thread
export -f optimize_thread
export -f drop_part_thread

TIMEOUT=60

#clickhouse_client_loop_timeout $TIMEOUT "create_drop_thread ${engines[@]}" &
clickhouse_client_loop_timeout $TIMEOUT insert_thread src &
clickhouse_client_loop_timeout $TIMEOUT insert_thread src &
clickhouse_client_loop_timeout $TIMEOUT insert_thread dst &
clickhouse_client_loop_timeout $TIMEOUT move_partition_src_dst_thread &
clickhouse_client_loop_timeout $TIMEOUT replace_partition_src_src_thread &
clickhouse_client_loop_timeout $TIMEOUT drop_partition_thread &
clickhouse_client_loop_timeout $TIMEOUT optimize_thread &
clickhouse_client_loop_timeout $TIMEOUT drop_part_thread &
wait

check_replication_consistency "dst_" "count(), sum(p), sum(k), sum(v)"
try_sync_replicas "src_"

for ((i=0; i<16; i++)) do
    $CLICKHOUSE_CLIENT -q "DROP TABLE dst_$i" 2>&1| grep -Fv "is already started to be removing" &
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS src_$i" 2>&1| grep -Fv "is already started to be removing" &
done

wait
