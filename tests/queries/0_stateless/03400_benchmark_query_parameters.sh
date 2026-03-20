#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test basic string parameter
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param_name 'test_value' \
    --query "SELECT {name:String} AS result" \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Basic string parameter"

# Test numeric parameter
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param_id 42 \
    --query "SELECT {id:UInt32} AS result" \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Numeric parameter"

# Test array parameter
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param_arr '[1,2,3]' \
    --query "SELECT {arr:Array(UInt32)} AS result" \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Array parameter"

# Test multiple parameters
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param_str 'hello' \
    --param_num 123 \
    --query "SELECT {str:String} AS s, {num:UInt32} AS n" \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Multiple parameters"

# Test identifier parameter (table name)
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param_tbl 'one' \
    --query "SELECT * FROM system.{tbl:Identifier}" \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Identifier parameter"

# Test space-separated parameter form (--param_name value)
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param_val test_space \
    --query "SELECT {val:String} AS result" \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Space-separated parameter"

# Test hyphen variant (--param-name)
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param-hyphen 'hyphen_test' \
    --query "SELECT {hyphen:String} AS result" \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Hyphen variant parameter"

echo "All tests passed"
