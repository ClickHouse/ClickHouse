#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel, no-fasttest
# Tag no-fasttest: max_memory_usage_for_user can interfere another queries running concurrently

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

MISTER_USER="test_01238_http_$RANDOM"
# Create another user to check limit on the amount of memory reliabily
${CLICKHOUSE_CLIENT} --format Null -q "CREATE USER $MISTER_USER"

# This is needed to keep at least one running query for user for the time of test.
# (1k http queries takes ~1 second, let's run for 5x more to avoid flaps)
${CLICKHOUSE_CLIENT} --user ${MISTER_USER} --function_sleep_max_microseconds_per_block 5000000 --format Null <<<'SELECT sleepEachRow(1) FROM numbers(5)' &

# ignore "yes: standard output: Broken pipe"
yes 'SELECT 1' 2>/dev/null | {
    head -n1000
} | {
    xargs -I{} ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${MISTER_USER}&wait_end_of_query=1&max_memory_usage_for_user=$((1<<30))" -d '{}'
} | grep -x -c 1

wait

# Reset max_memory_usage_for_user, so it will not affect other tests
${CLICKHOUSE_CLIENT} --user ${MISTER_USER} --max_memory_usage_for_user=0 -q "SELECT 1 FORMAT Null"
