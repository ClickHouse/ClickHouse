#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS table_to_rename"

$CLICKHOUSE_CLIENT --query="CREATE TABLE table_to_rename(v UInt64, v1 UInt64)ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0"

$CLICKHOUSE_CLIENT --query="INSERT INTO table_to_rename VALUES (1, 1)"


# we want to following mutations to stuck
# That is why we stop merges and wait in loops until they actually start
$CLICKHOUSE_CLIENT --query="SYSTEM STOP MERGES table_to_rename"

$CLICKHOUSE_CLIENT --query="ALTER TABLE table_to_rename RENAME COLUMN v1 to v2" &

counter=0 retries=60

I=0
while [[ $counter -lt $retries ]]; do
    I=$((I + 1))
    result=$($CLICKHOUSE_CLIENT --query "show create table table_to_rename")
    if [[ $result == *"v2"* ]]; then
        break;
    fi
    sleep 0.1
    ((++counter))
done


$CLICKHOUSE_CLIENT --query="ALTER TABLE table_to_rename UPDATE v2 = 77 WHERE 1 = 1 SETTINGS mutations_sync = 2" &


# we cannot wait in the same way like we do for previous alter
# because it's metadata alter and this one will wait for it
sleep 3

$CLICKHOUSE_CLIENT --query="SYSTEM START MERGES table_to_rename"

wait

$CLICKHOUSE_CLIENT --query="SELECT * FROM table_to_rename FORMAT JSONEachRow"


 $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS table_to_rename"
