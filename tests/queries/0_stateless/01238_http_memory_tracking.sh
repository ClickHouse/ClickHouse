#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# TODO: the test can be way more optimal

set -o pipefail

function execute_null()
{
    ${CLICKHOUSE_CLIENT} --format Null -n "$@"
}

# This is needed to keep at least one running query for user for the time of test.
# (10k http queries takes 10seconds, let's run for 3x more to avoid flaps)
execute_null <<<'SELECT sleepEachRow(1) FROM numbers(30)' &

# ignore "yes: standard output: Broken pipe"
yes 'SELECT 1' 2>/dev/null | {
    head -n10000
} | {
    xargs -P10000 -i ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&max_memory_usage_for_user=$((10<<30))" -d '{}'
} | grep -x -c 1

wait
