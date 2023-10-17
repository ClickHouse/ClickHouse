#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_COUNT_ORIGIN_BLOCKS="SELECT COUNT(*) FROM system.parts WHERE database = currentDatabase() AND table = 'block_dedup_token' AND min_block_number == max_block_number;"
QUERY_SELECT_FROM_TABLE_ORDERED="SELECT * FROM block_dedup_token ORDER BY id;"
INSERT_BLOCK_SETTINGS="max_insert_block_size=1&min_insert_block_size_rows=0&min_insert_block_size_bytes=0"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS block_dedup_token SYNC"
$CLICKHOUSE_CLIENT --query="CREATE TABLE block_dedup_token (id Int32) ENGINE=MergeTree() ORDER BY id SETTINGS non_replicated_deduplication_window=0xFFFFFFFF;"

$CLICKHOUSE_CLIENT --query="SELECT 'insert 2 blocks with dedup token, 1 row per block'"
DEDUP_TOKEN='dedup1'
echo 'INSERT INTO block_dedup_token VALUES (1), (2)' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$INSERT_BLOCK_SETTINGS&insert_deduplication_token='$DEDUP_TOKEN'&query=" --data-binary @-
$CLICKHOUSE_CLIENT --multiquery --query "$QUERY_COUNT_ORIGIN_BLOCKS;$QUERY_SELECT_FROM_TABLE_ORDERED"

$CLICKHOUSE_CLIENT --query="SELECT 'insert deduplicated by token'"
echo 'INSERT INTO block_dedup_token VALUES (1), (2)' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$INSERT_BLOCK_SETTINGS&insert_deduplication_token='$DEDUP_TOKEN'&query=" --data-binary @-
$CLICKHOUSE_CLIENT --multiquery --query "$QUERY_COUNT_ORIGIN_BLOCKS;$QUERY_SELECT_FROM_TABLE_ORDERED"

$CLICKHOUSE_CLIENT --query="SELECT 'insert the same data by providing different dedup token'"
DEDUP_TOKEN='dedup2'
echo 'INSERT INTO block_dedup_token VALUES (1), (2)' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$INSERT_BLOCK_SETTINGS&insert_deduplication_token='$DEDUP_TOKEN'&query=" --data-binary @-
$CLICKHOUSE_CLIENT --multiquery --query "$QUERY_COUNT_ORIGIN_BLOCKS;$QUERY_SELECT_FROM_TABLE_ORDERED"

$CLICKHOUSE_CLIENT --query="SELECT 'insert 4 blocks, 2 deduplicated, 2 inserted'"
echo 'INSERT INTO block_dedup_token VALUES (1), (2), (3), (4)' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$INSERT_BLOCK_SETTINGS&insert_deduplication_token='$DEDUP_TOKEN'&query=" --data-binary @-
$CLICKHOUSE_CLIENT --multiquery --query "$QUERY_COUNT_ORIGIN_BLOCKS;$QUERY_SELECT_FROM_TABLE_ORDERED"

$CLICKHOUSE_CLIENT --query="SELECT 'disable token based deduplication, insert the same data as with token'"
DEDUP_TOKEN=''
echo 'INSERT INTO block_dedup_token VALUES (1), (2), (3), (4)' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$INSERT_BLOCK_SETTINGS&insert_deduplication_token='$DEDUP_TOKEN'&query=" --data-binary @-
$CLICKHOUSE_CLIENT --multiquery --query "$QUERY_COUNT_ORIGIN_BLOCKS;$QUERY_SELECT_FROM_TABLE_ORDERED"

$CLICKHOUSE_CLIENT --query="DROP TABLE block_dedup_token SYNC"
