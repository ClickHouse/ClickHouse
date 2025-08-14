#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --max_result_bytes 0 --max_memory_usage_in_client=1 -q "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { clientError MEMORY_LIMIT_EXCEEDED }"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client=0 -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"

$CLICKHOUSE_CLIENT --max_result_bytes 0 --max_memory_usage_in_client='5K' -q "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { clientError MEMORY_LIMIT_EXCEEDED }"
$CLICKHOUSE_CLIENT --max_result_bytes 0 --max_memory_usage_in_client='5k' -q "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { clientError MEMORY_LIMIT_EXCEEDED }"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='50M' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='23G' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='11T' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"

$CLICKHOUSE_CLIENT --max_memory_usage_in_client='2P' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='2.1p' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='10E' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='10.2e' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='-1.1T' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_NUMBER"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='-1' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_NUMBER"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='1m' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='14g' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='11t' -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
