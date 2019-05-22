#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

cur_name=${BASH_SOURCE[0]}

settings="$server_logs --log_queries=1 --log_query_threads=1 --log_profile_events=1 --log_query_settings=1"

# Test insert logging on each block and checkPacket() method

$CLICKHOUSE_CLIENT $settings -n -q "
DROP TABLE IF EXISTS merge_tree_table;
CREATE TABLE merge_tree_table (id UInt64, date Date, uid UInt32) ENGINE = MergeTree(date, id, 8192);"

$CLICKHOUSE_CLIENT $settings -q "INSERT INTO merge_tree_table SELECT (intHash64(number)) % 10000, toDate('2018-08-01'), rand() FROM system.numbers LIMIT 10000000;"

$CLICKHOUSE_CLIENT $settings -q "OPTIMIZE TABLE merge_tree_table FINAL;"

toching_many_parts_query="SELECT count() from (SELECT toDayOfWeek(date) as m, id, count() FROM merge_tree_table GROUP BY id, m ORDER BY count() DESC LIMIT 10 SETTINGS max_threads = 1)"
$CLICKHOUSE_CLIENT $settings -q "$toching_many_parts_query" &> /dev/null

$CLICKHOUSE_CLIENT $settings -q "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT $settings -q "SELECT pi.Values FROM system.query_log ARRAY JOIN ProfileEvents as pi WHERE query='$toching_many_parts_query' and pi.Names = 'FileOpen' ORDER BY event_time DESC LIMIT 1;"

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE IF EXISTS merge_tree_table;"
