#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CURRENT_DATABASE=$($CLICKHOUSE_CLIENT -q "SELECT currentDatabase()")

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CURRENT_DATABASE}.t03755_parquet_insert_with_batches;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CURRENT_DATABASE}.t03755_parquet_insert_with_batches (c0 JSON) ENGINE = MergeTree() ORDER BY tuple();"
    
$CLICKHOUSE_CLIENT -q "INSERT INTO TABLE FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+${CURRENT_DATABASE}.t03755_parquet_insert_with_batches+(c0)+FORMAT+Parquet', 'Parquet', 'c0 JSON') SELECT '{\"c0\":1}' FROM numbers(10) SETTINGS output_format_parquet_batch_size = 4;"
$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM ${CURRENT_DATABASE}.t03755_parquet_insert_with_batches;"