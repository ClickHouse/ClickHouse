#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh
set -eo pipefail

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS view1;
DROP TABLE IF EXISTS table1;
CREATE TABLE table1 (number UInt64) ENGINE=MergeTree ORDER BY number SETTINGS index_granularity=1;
INSERT INTO table1 SELECT number FROM numbers(1, 300);
CREATE VIEW view1 AS SELECT number FROM table1;
EOF

# The following SELECT is expected to read 20 rows. In fact it may decide to read more than 20 rows, but not too many anyway.
# So we'll check that the number of read rows is less than 40.
query="SELECT * FROM (SELECT * FROM view1) ORDER BY number DESC LIMIT 20 SETTINGS max_streams_for_merge_tree_reading = 1"

query_id=${CLICKHOUSE_DATABASE}_optimize_read_in_order_from_view_$RANDOM$RANDOM

$CLICKHOUSE_CLIENT -q "$query" --query_id="$query_id" --log_queries=1 --optimize_read_in_order=1

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

read_rows=$($CLICKHOUSE_CLIENT -q "SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query_id='${query_id}' AND type='QueryFinish'")

if [ -z "$read_rows" ]; then
    echo "read_rows:not found"
elif (( "$read_rows" > 40 )); then
    echo "read_rows:$read_rows"
else
    echo "read_rows:ok"
fi

query_plan=$($CLICKHOUSE_CLIENT -q "EXPLAIN actions=1 $query" --optimize_read_in_order=1)

echo "$query_plan" | grep -A 1 "ReadFromMergeTree" | sed 's/^[ \t]*//'

$CLICKHOUSE_CLIENT -q "DROP TABLE view1"
$CLICKHOUSE_CLIENT -q "DROP TABLE table1"
