#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into function file(02305_data1.jsonl) select NULL as x from numbers(10) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02305_data2.jsonl) select NULL as x from numbers(10) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02305_data3.jsonl) select NULL as x from numbers(10) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "insert into function file(02305_data4.jsonl) select number % 2 ? number : NULL as x from numbers(10) settings engine_file_truncate_on_insert=1"

$CLICKHOUSE_CLIENT -q "desc file('02305_data*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=8" 2>&1 | grep -c 'ONLY_NULLS_WHILE_READING_SCHEMA';
$CLICKHOUSE_CLIENT -q "desc file('02305_data*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=16" 2>&1 | grep -c 'ONLY_NULLS_WHILE_READING_SCHEMA';
$CLICKHOUSE_CLIENT -q "desc file('02305_data*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=24" 2>&1 | grep -c 'ONLY_NULLS_WHILE_READING_SCHEMA';
$CLICKHOUSE_CLIENT -q "desc file('02305_data*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=31" 2>&1 | grep -c 'ONLY_NULLS_WHILE_READING_SCHEMA';
$CLICKHOUSE_CLIENT -q "desc file('02305_data*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=32"
$CLICKHOUSE_CLIENT -q "desc file('02305_data*.jsonl') settings input_format_max_rows_to_read_for_schema_inference=100"
