#!/usr/bin/env bash
# Tags: race, no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS table_for_concurrent_alter"

$CLICKHOUSE_CLIENT --query="CREATE TABLE table_for_concurrent_alter (id UInt64, Data String) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity=4096;";

n=0
while [ "$n" -lt 50 ];
do
    n=$(( n + 1 ))
    $CLICKHOUSE_CLIENT --query="INSERT INTO table_for_concurrent_alter VALUES(1, 'Hello')" > /dev/null 2> /dev/null &
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE table_for_concurrent_alter FINAL" > /dev/null 2> /dev/null &
done &


q=0
while [ "$q" -lt 50 ];
do
    q=$(( q + 1 ))
    counter=$(( 100 + q ))
    $CLICKHOUSE_CLIENT --query="ALTER TABLE table_for_concurrent_alter MODIFY SETTING parts_to_throw_insert = $counter, parts_to_delay_insert = $counter, min_merge_bytes_to_use_direct_io = $counter" > /dev/null 2> /dev/null &
done &

sleep 4

# if we have too many simultaneous queries
until $CLICKHOUSE_CLIENT --query "SELECT 1" 2>/dev/null 1>/dev/null
do
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "SELECT 1"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS table_for_concurrent_alter"
