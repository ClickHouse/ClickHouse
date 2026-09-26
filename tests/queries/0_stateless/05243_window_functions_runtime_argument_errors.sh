#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Both functions reject the argument only while the query runs. The error must name the function and carry
# the error code.

out=$(${CLICKHOUSE_CLIENT} --query "SELECT nth_value(number, 0) OVER () FROM numbers(3)" 2>&1)
echo "$out" | grep -o -F --max-count 1 'function nth_value must be in'
echo "$out" | grep -o -F --max-count 1 'BAD_ARGUMENTS'

out=$(${CLICKHOUSE_CLIENT} --query "SELECT ntile(0) OVER (ORDER BY number) FROM numbers(3)" 2>&1)
echo "$out" | grep -o -F --max-count 1 "Argument of 'ntile' function must be greater than zero"
echo "$out" | grep -o -F --max-count 1 'BAD_ARGUMENTS'
