#!/usr/bin/env bash
# Tags: long, no-s3-storage

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--log_queries=1 --log_query_threads=1 --log_profile_events=1 --log_query_settings=1 --allow_deprecated_syntax_for_merge_tree=1"

# Test insert logging on each block and checkPacket() method

$CLICKHOUSE_CLIENT $settings -n -q "
DROP TABLE IF EXISTS merge_tree_table;
CREATE TABLE merge_tree_table (id UInt64, date Date, uid UInt32) ENGINE = MergeTree(date, id, 8192);"


$CLICKHOUSE_CLIENT $settings -q "INSERT INTO merge_tree_table SELECT (intHash64(number)) % 10000, toDate('2018-08-01'), rand() FROM system.numbers LIMIT 10000000;"

# If merge is already happening, OPTIMIZE will be noop. But we have to ensure that the data is merged.
for _ in {1..100}; do $CLICKHOUSE_CLIENT $settings --optimize_throw_if_noop=1 -q "OPTIMIZE TABLE merge_tree_table FINAL;" && break; sleep 1; done

# The query may open more files if query log will be flushed during the query.
# To lower this chance, we also flush logs before the query.
$CLICKHOUSE_CLIENT $settings -q "SYSTEM FLUSH LOGS"

touching_many_parts_query="SELECT count() FROM (SELECT toDayOfWeek(date) AS m, id, count() FROM merge_tree_table GROUP BY id, m ORDER BY count() DESC LIMIT 10 SETTINGS max_threads = 1)"
$CLICKHOUSE_CLIENT $settings -q "$touching_many_parts_query" &> /dev/null

$CLICKHOUSE_CLIENT $settings -q "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT $settings -q "SELECT ProfileEvents['FileOpen'] as opened_files FROM system.query_log WHERE query='$touching_many_parts_query' and current_database = currentDatabase() ORDER BY event_time DESC, opened_files DESC LIMIT 1;"

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE IF EXISTS merge_tree_table;"
