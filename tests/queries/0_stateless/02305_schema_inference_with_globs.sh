#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into function file(data_${CLICKHOUSE_TEST_UNIQUE_NAME}_1.jsonl) select NULL as x from numbers(10)"
$CLICKHOUSE_CLIENT -q "insert into function file(data_${CLICKHOUSE_TEST_UNIQUE_NAME}_2.jsonl) select NULL as x from numbers(10)"
$CLICKHOUSE_CLIENT -q "insert into function file(data_${CLICKHOUSE_TEST_UNIQUE_NAME}_3.jsonl) select NULL as x from numbers(10)"
$CLICKHOUSE_CLIENT -q "desc file('data_${CLICKHOUSE_TEST_UNIQUE_NAME}_*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=8" 2>&1 | grep -c 'ONLY_NULLS_WHILE_READING_SCHEMA';
$CLICKHOUSE_CLIENT -q "desc file('data_${CLICKHOUSE_TEST_UNIQUE_NAME}_*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=16" 2>&1 | grep -c 'ONLY_NULLS_WHILE_READING_SCHEMA';
$CLICKHOUSE_CLIENT -q "desc file('data_${CLICKHOUSE_TEST_UNIQUE_NAME}_*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=24" 2>&1 | grep -c 'ONLY_NULLS_WHILE_READING_SCHEMA';

$CLICKHOUSE_CLIENT -q "insert into function file(data_${CLICKHOUSE_TEST_UNIQUE_NAME}_4.jsonl) select number % 2 ? number : NULL as x from numbers(10)"
$CLICKHOUSE_CLIENT -q "desc file('data_${CLICKHOUSE_TEST_UNIQUE_NAME}_*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=32"
$CLICKHOUSE_CLIENT -q "desc file('data_${CLICKHOUSE_TEST_UNIQUE_NAME}_*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=100"
