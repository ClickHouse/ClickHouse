#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# Tag no-ordinary-database: KeeperMap doesn't support Ordinary database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS keeper_map_with_filter;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE keeper_map_with_filter (key String, value String) ENGINE=KeeperMap(concat(currentDatabase(), '_simple')) PRIMARY KEY key;"
$CLICKHOUSE_CLIENT --query="INSERT INTO keeper_map_with_filter (*) SELECT n.number, n.number*10 FROM numbers(10) n;"

$CLICKHOUSE_CLIENT --query "EXPLAIN actions=1 SELECT value FROM keeper_map_with_filter LIMIT 1" | grep -A 2 "ReadFromKeeperMap"
$CLICKHOUSE_CLIENT --query "EXPLAIN actions=1,optimize=0 SELECT value FROM keeper_map_with_filter" | grep -A 2 "ReadFromKeeperMap" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM keeper_map_with_filter WHERE key = '5'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM keeper_map_with_filter WHERE key = '5' FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"
$CLICKHOUSE_CLIENT --query "EXPLAIN actions=1 SELECT value FROM keeper_map_with_filter WHERE key = '5'" | grep -A 3 "ReadFromKeeperMap"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM keeper_map_with_filter WHERE key = '5' OR key = '6'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM keeper_map_with_filter WHERE key = '5' OR key = '6' FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT "--param_key=5" --query "SELECT count() FROM keeper_map_with_filter WHERE key = {key:String}"
$CLICKHOUSE_CLIENT "--param_key=5" --query "SELECT value FROM keeper_map_with_filter WHERE key = {key:String} FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM keeper_map_with_filter WHERE key IN ('5', '6')"
$CLICKHOUSE_CLIENT --query "SELECT value FROM keeper_map_with_filter WHERE key IN ('5', '6') FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query="DROP TABLE keeper_map_with_filter;"

# Same test, but with complex key
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS keeper_map_with_filter_and_complex_key;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE keeper_map_with_filter_and_complex_key (key String, value String) ENGINE=KeeperMap(concat(currentDatabase(), '_complex')) PRIMARY KEY hex(toFloat32(key));"
$CLICKHOUSE_CLIENT --query="INSERT INTO keeper_map_with_filter_and_complex_key (*) SELECT n.number, n.number*10 FROM numbers(10) n;"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM keeper_map_with_filter_and_complex_key WHERE key = '5'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM keeper_map_with_filter_and_complex_key WHERE key = '5' FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"
$CLICKHOUSE_CLIENT --query "EXPLAIN actions=1 SELECT value FROM keeper_map_with_filter_and_complex_key WHERE key = '5'" | grep -A 3 "ReadFromKeeperMap"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM keeper_map_with_filter_and_complex_key WHERE key = '5' OR key = '6'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM keeper_map_with_filter_and_complex_key WHERE key = '5' OR key = '6' FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT "--param_key=5" --query "SELECT count() FROM keeper_map_with_filter_and_complex_key WHERE key = {key:String}"
$CLICKHOUSE_CLIENT "--param_key=5" --query "SELECT value FROM keeper_map_with_filter_and_complex_key WHERE key = {key:String} FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM keeper_map_with_filter_and_complex_key WHERE key IN ('5', '6')"
$CLICKHOUSE_CLIENT --query "SELECT value FROM keeper_map_with_filter_and_complex_key WHERE key IN ('5', '6') FORMAT JSON" | grep "rows_read" | tr -d "[:blank:]"

$CLICKHOUSE_CLIENT --query="DROP TABLE keeper_map_with_filter_and_complex_key;"
