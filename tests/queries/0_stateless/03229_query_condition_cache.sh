#!/usr/bin/env bash
# Tags: no-parallel
set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} -q "SYSTEM DROP QUERY CONDITION CACHE"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS tab SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO tab SELECT number, number FROM numbers(1000000)"

query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true" "--query_id=$query_id"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -q "SELECT ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses'], toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal']) FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query_id = '$query_id'"

query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} -q "SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true" "--query_id=$query_id"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -q "SELECT ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses'], toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal']) FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query_id = '$query_id'"


${CLICKHOUSE_CLIENT} -q "SELECT 'Test optimize_move_to_prewhere=false'"
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP QUERY CONDITION CACHE"

query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} -q "SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere=false" "--query_id=$query_id"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -q "SELECT ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses'], toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal']) FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query_id = '$query_id'"

# WHERE condition will hit the query condition cache.
query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} -q "SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere=false;" "--query_id=$query_id"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -q "SELECT ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses'], toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal']) FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query_id = '$query_id'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS tab"
