#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}_data2.jsonl') select NULL as x SETTINGS engine_file_truncate_on_insert = 1";
$CLICKHOUSE_CLIENT -q "insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}_data3.jsonl') select * from numbers(0) SETTINGS engine_file_truncate_on_insert = 1";
$CLICKHOUSE_CLIENT -q "insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}_data4.jsonl') select 1 as x SETTINGS engine_file_truncate_on_insert = 1";

$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}_data*.jsonl') order by x";

$CLICKHOUSE_CLIENT -q "insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}_data4.jsonl', 'TSV') select 1 as x";
$CLICKHOUSE_CLIENT -q "insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}_data1.jsonl', 'TSV') select [1,2,3] as x SETTINGS engine_file_truncate_on_insert = 1";

$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}_data*.jsonl') settings schema_inference_use_cache_for_file=0" 2>&1 | grep -F -q "CANNOT_PARSE_INPUT_ASSERTION_FAILED" && echo "OK" || echo "FAIL";

