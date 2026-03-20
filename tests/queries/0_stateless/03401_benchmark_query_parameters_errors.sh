#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test missing parameter value (param as last argument)
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --query "SELECT 1" \
    --param_missing \
    2>&1 | grep -q "Parameter requires value" && echo "OK: Missing parameter value detected"

# Test parameter value that looks like a flag (--param_name --query should error, not consume --query)
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --param_test --query "SELECT 1" \
    2>&1 | grep -q "looks like a flag" && echo "OK: Flag-like parameter value detected"

# Test query with undefined parameter (should fail gracefully)
${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    --query "SELECT {undefined:String}" \
    2>&1 | grep -qi "error\|exception" && echo "OK: Undefined parameter detected"

echo "Error handling tests passed"
