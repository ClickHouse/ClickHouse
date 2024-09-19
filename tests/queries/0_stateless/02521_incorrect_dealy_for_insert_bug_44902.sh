#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02521_insert_delay"
# Create MergeTree with settings which allow to insert maximum 5 parts, on 6th it'll throw TOO_MANY_PARTS
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02521_insert_delay (key UInt32, value String) Engine=MergeTree() ORDER BY tuple() SETTINGS parts_to_delay_insert=1, parts_to_throw_insert=5, max_delay_to_insert=1, min_delay_to_insert_ms=300"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES test_02521_insert_delay"

# Every delay is increased by max_delay_to_insert*1000/(parts_to_throw_insert - parts_to_delay_insert + 1), here it's 250ms
# 0-indexed INSERT - no delay, 1-indexed INSERT - 300ms instead of 250ms due to min_delay_to_insert_ms
for i in {0..4}
do
    query_id="${CLICKHOUSE_DATABASE}_02521_${i}_$RANDOM$RANDOM"
    $CLICKHOUSE_CLIENT --query_id="$query_id" --max_insert_threads 1 -q "INSERT INTO test_02521_insert_delay SELECT number, toString(number) FROM numbers(${i}, 1)"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
    $CLICKHOUSE_CLIENT --param_query_id="$query_id" -q "select ProfileEvents['DelayedInsertsMilliseconds'] as delay from system.query_log where event_date >= yesterday() and current_database = '$CLICKHOUSE_DATABASE' and query_id = {query_id:String} order by delay desc limit 1"
done

$CLICKHOUSE_CLIENT -q "INSERT INTO test_02521_insert_delay VALUES(0, 'This query throws error')" 2>&1 | grep -o 'TOO_MANY_PARTS' | head -n 1

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02521_insert_delay"
