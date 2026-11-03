#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper, no-fasttest

# Suppress server log packets (this test provokes routine ZNOTEMPTY cleanup warnings on the
# shared KeeperMap path); client exceptions are still printed to stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function create_drop_loop()
{
    table_name="02703_keeper_map_concurrent_$1"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS $table_name" || exit 1
    for _ in `seq $1`
    do
        sleep 0.3
    done

    i=0
    local TIMELIMIT=$((SECONDS+$2))
    while [ $SECONDS -lt "$TIMELIMIT" ];
    do
        $CLICKHOUSE_CLIENT --query="CREATE TABLE IF NOT EXISTS $table_name (key UInt64, value UInt64) ENGINE = KeeperMap('/02703_keeper_map/$CLICKHOUSE_DATABASE') PRIMARY KEY(key)" || exit 1
        # A failed INSERT leaves the previous iteration's value on the shared path, which the
        # SELECT below would then report as an invalid result.
        $CLICKHOUSE_CLIENT --query="INSERT INTO $table_name VALUES ($1, $i)" || exit 1
        result=
        result=$($CLICKHOUSE_CLIENT --query="SELECT value FROM $table_name WHERE key = $1") || exit 1

        if [ "$result" != "$i" ]
        then
            echo "Got invalid result $result"
            exit 1
        fi

        $CLICKHOUSE_CLIENT --query="DROP TABLE $table_name" || exit 1

        ((++i))
    done
}

export -f create_drop_loop;

THREADS=10
TIMEOUT=20

pids=()
for i in `seq $THREADS`
do
    create_drop_loop $i $TIMEOUT &
    pids+=("$!")
done

exit_code=0
for pid in "${pids[@]}"
do
    wait "$pid" || exit_code=1
done

for i in `seq $THREADS`
do
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS 02703_keeper_map_concurrent_$i"
done

$CLICKHOUSE_CLIENT --query="SELECT count() FROM system.zookeeper WHERE path = '/test_keeper_map/02703_keeper_map/$CLICKHOUSE_DATABASE'"

exit $exit_code
