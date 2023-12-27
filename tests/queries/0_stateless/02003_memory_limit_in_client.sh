#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --max_memory_usage_in_client=1 -n -q "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { clientError MEMORY_LIMIT_EXCEEDED }"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client=0 -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"

$CLICKHOUSE_CLIENT --max_memory_usage_in_client='5K' -n -q "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { clientError MEMORY_LIMIT_EXCEEDED }"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='5k' -n -q "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { clientError MEMORY_LIMIT_EXCEEDED }"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='1M' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='23G' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='11T' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000"

$CLICKHOUSE_CLIENT --max_memory_usage_in_client='2P' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='2.1p' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='10E' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='10.2e' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='-1.1T' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_NUMBER"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='-1' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_NUMBER"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='1m' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='14g' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT --max_memory_usage_in_client='11t' -n -q "SELECT * FROM (SELECT * FROM system.numbers LIMIT 600000) as num WHERE num.number=60000" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
