#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# shellcheck disable=SC2154

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "CREATE TABLE sample_final (CounterID UInt32, EventDate Date, EventTime DateTime, UserID UInt64, Sign Int8) ENGINE = CollapsingMergeTree(Sign) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime) SAMPLE BY intHash32(UserID)"

$CLICKHOUSE_CLIENT -q "INSERT INTO sample_final SELECT number / (8192 * 4), toDate('2019-01-01'), toDateTime('2019-01-01 00:00:01') + number, number / (8192 * 2), if((number % 3) = 1, -1, 1) FROM numbers(1000000)"

query_id="${CLICKHOUSE_DATABASE}_final_excessive_reading_bug_$RANDOM"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "select * from sample_final FINAL SAMPLE 1/2 OFFSET 1/2 format Null settings max_threads=16"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --param_query_id="$query_id" -q "
SELECT ProfileEvents['SelectedRows'] < 1_000_000
  FROM system.query_log
 WHERE event_date >= yesterday() AND type = 'QueryFinish' AND query_id = {query_id:String} AND current_database = currentDatabase()"
