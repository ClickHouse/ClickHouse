#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function create_drop_loop()
{
    table_name="02703_keeper_map_concurrent_$1"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS $table_name"
    for _ in `seq $1`
    do
        sleep 0.3
    done

    i=0
    local TIMELIMIT=$((SECONDS+$2))
    while [ $SECONDS -lt "$TIMELIMIT" ];
    do
        $CLICKHOUSE_CLIENT --query="CREATE TABLE IF NOT EXISTS $table_name (key UInt64, value UInt64) ENGINE = KeeperMap('/02703_keeper_map/$CLICKHOUSE_DATABASE') PRIMARY KEY(key)"
        $CLICKHOUSE_CLIENT --query="INSERT INTO $table_name VALUES ($1, $i)"
        result=$($CLICKHOUSE_CLIENT --query="SELECT value FROM $table_name WHERE key = $1")

        if [ $result != $i ]
        then
            echo "Got invalid result $result"
            exit 1
        fi

        $CLICKHOUSE_CLIENT --query="DROP TABLE $table_name"

        ((++i))
    done
}

export -f create_drop_loop;

THREADS=10
TIMEOUT=20

for i in `seq $THREADS`
do
    create_drop_loop $i $TIMEOUT 2> /dev/null &
done

wait

for i in `seq $THREADS`
do
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS 02703_keeper_map_concurrent_$i"
done

$CLICKHOUSE_CLIENT --query="SELECT count() FROM system.zookeeper WHERE path = '/test_keeper_map/02703_keeper_map/$CLICKHOUSE_DATABASE'"
