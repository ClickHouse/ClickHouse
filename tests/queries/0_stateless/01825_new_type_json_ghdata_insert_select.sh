#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, long, no-asan, no-tsan, no-msan
# ^ no-object-storage: it is memory-hungry, no-{a,t,m}san: too long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_string"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_from_string"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata_2 (data JSON(max_dynamic_paths=100)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = '1Mi'"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata_2_string (data String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = '1Mi'"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata_2_from_string (data JSON(max_dynamic_paths=100)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = '1Mi'"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES ghdata_2"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES ghdata_2_from_string"

cat $CUR_DIR/data_json/ghdata_sample.json | ${CLICKHOUSE_CLIENT} --max_insert_block_size 100 --min_insert_block_size_rows 100 --min_insert_block_size_bytes 100Mi -q "INSERT INTO ghdata_2 FORMAT JSONAsObject"
cat $CUR_DIR/data_json/ghdata_sample.json | ${CLICKHOUSE_CLIENT} --max_insert_block_size 100 --min_insert_block_size_rows 100 --min_insert_block_size_bytes 100Mi -q "INSERT INTO ghdata_2_string FORMAT JSONAsString"

${CLICKHOUSE_CLIENT} --merge_tree_min_rows_for_concurrent_read 1 --merge_tree_min_bytes_for_concurrent_read 1 --max_insert_block_size 100 --min_insert_block_size_rows 100 --min_insert_block_size_bytes 100Mi -q "INSERT INTO ghdata_2_from_string SELECT data FROM ghdata_2_string"

${CLICKHOUSE_CLIENT} --merge_tree_min_rows_for_concurrent_read 1 --merge_tree_min_bytes_for_concurrent_read 1 --max_block_size 100 -q "SELECT count(), sum(cityHash64(toString(data))) FROM ghdata_2_from_string"
${CLICKHOUSE_CLIENT} --merge_tree_min_rows_for_concurrent_read 1 --merge_tree_min_bytes_for_concurrent_read 1 --max_block_size 100 -q "SELECT count(), sum(cityHash64(toString(data))) FROM ghdata_2"

${CLICKHOUSE_CLIENT} --merge_tree_min_rows_for_concurrent_read 1 --merge_tree_min_bytes_for_concurrent_read 1 --max_block_size 100 -q "SELECT arrayJoin(distinctJSONPaths(data)) AS path FROM ghdata_2_from_string ORDER BY path"
${CLICKHOUSE_CLIENT} --merge_tree_min_rows_for_concurrent_read 1 --merge_tree_min_bytes_for_concurrent_read 1 --max_block_size 100 -q "SELECT arrayJoin(distinctJSONPaths(data)) AS path FROM ghdata_2 ORDER BY path"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_string"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_from_string"
