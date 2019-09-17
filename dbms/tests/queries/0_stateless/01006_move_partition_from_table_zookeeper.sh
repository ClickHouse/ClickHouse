#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

function query_with_retry
{
    retry=0
    until [ $retry -ge 5 ]
    do
        result=`$CLICKHOUSE_CLIENT $2 --query="$1" 2>&1`
        if [ "$?" == 0 ]; then
            echo -n $result
            return
        else
            retry=$(($retry + 1))
            sleep 3
        fi
    done
    echo "Query '$1' failed with '$result'"
}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.dst;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test.src (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/src', '1') PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.dst (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/dst', '1') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test.src VALUES (0, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test.src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test.src VALUES (1, '1', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test.src VALUES (2, '0', 1);"

$CLICKHOUSE_CLIENT --query="SELECT 'Initial';"
$CLICKHOUSE_CLIENT --query="INSERT INTO test.dst VALUES (0, '1', 2);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test.dst VALUES (1, '1', 2), (1, '2', 2);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test.dst VALUES (2, '1', 2);"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA test.dst;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM test.src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM test.dst;"


$CLICKHOUSE_CLIENT --query="SELECT 'MOVE simple';"
query_with_retry "ALTER TABLE test.src MOVE PARTITION 1 TO TABLE test.dst;"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA test.dst;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM test.src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM test.dst;"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.dst;"
