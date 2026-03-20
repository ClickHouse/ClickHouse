#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test with high concurrency to ensure thread safety
${CLICKHOUSE_BENCHMARK} --iterations 100 --concurrency 10 \
    --param_value 'concurrent_test' \
    --query "SELECT {value:String}, sleep(0.001)" \
    2>&1 | grep -q "Queries executed: 100" && echo "OK: High concurrency with parameters"

# Test with randomize option
${CLICKHOUSE_BENCHMARK} --iterations 50 --concurrency 5 --randomize \
    --param_num 42 \
    --query "SELECT {num:UInt32} * number FROM system.numbers LIMIT 10" \
    2>&1 | grep -q "Queries executed: 50" && echo "OK: Randomized execution with parameters"

echo "Concurrency tests passed"
