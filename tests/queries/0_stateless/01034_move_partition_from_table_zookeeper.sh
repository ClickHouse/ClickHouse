#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

function query_with_retry
{
    retry=0
    until [ $retry -ge 5 ]
    do
        result=$($CLICKHOUSE_CLIENT $2 --query="$1" 2>&1)
        if [ "$?" == 0 ]; then
            echo -n "$result"
            return
        else
            retry=$(($retry + 1))
            sleep 3
        fi
    done
    echo "Query '$1' failed with '$result'"
}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dst;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/src1', '1') PARTITION BY p ORDER BY k;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/dst1', '1') PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (0, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '1', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (2, '0', 1);"

$CLICKHOUSE_CLIENT --query="SELECT 'Initial';"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst VALUES (0, '1', 2);"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst VALUES (1, '1', 2), (1, '2', 2);"
$CLICKHOUSE_CLIENT --query="INSERT INTO dst VALUES (2, '1', 2);"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst;"


$CLICKHOUSE_CLIENT --query="SELECT 'MOVE simple';"
query_with_retry "ALTER TABLE src MOVE PARTITION 1 TO TABLE dst;"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst;"

$CLICKHOUSE_CLIENT --query="DROP TABLE src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst;"

$CLICKHOUSE_CLIENT --query="SELECT 'MOVE incompatible schema missing column';"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/src2', '1') PARTITION BY p ORDER BY (d, p);"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst (p UInt64, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/dst2', '1') PARTITION BY p ORDER BY (d, p) SETTINGS old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (0, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '1', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (2, '0', 1);"

query_with_retry "ALTER TABLE src MOVE PARTITION 1 TO TABLE dst;" &>-
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst;"

$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst;"

$CLICKHOUSE_CLIENT --query="DROP TABLE src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst;"

$CLICKHOUSE_CLIENT --query="SELECT 'MOVE incompatible schema different order by';"

$CLICKHOUSE_CLIENT --query="CREATE TABLE src (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/src3', '1') PARTITION BY p ORDER BY (p, k, d);"
$CLICKHOUSE_CLIENT --query="CREATE TABLE dst (p UInt64, k String, d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_DATABASE/dst3', '1') PARTITION BY p ORDER BY (d, k, p);"


$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (0, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '0', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (1, '1', 1);"
$CLICKHOUSE_CLIENT --query="INSERT INTO src VALUES (2, '0', 1);"

query_with_retry "ALTER TABLE src MOVE PARTITION 1 TO TABLE dst;" &>-
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA dst;"

$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM src;"
$CLICKHOUSE_CLIENT --query="SELECT count(), sum(d) FROM dst;"

$CLICKHOUSE_CLIENT --query="DROP TABLE src;"
$CLICKHOUSE_CLIENT --query="DROP TABLE dst;"

