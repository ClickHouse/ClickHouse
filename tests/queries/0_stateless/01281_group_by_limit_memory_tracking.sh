#!/usr/bin/env bash

# Regression for MemoryTracker that had been incorrectly accounted
# (it was reseted before deallocation)
#
# For this will be used:
# - two-level group by
# - max_memory_usage_for_user
# - one users' query in background (to avoid reseting max_memory_usage_for_user)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -o pipefail

function execute_null()
{
    ${CLICKHOUSE_CLIENT} --format Null -n "$@"
}

function execute_group_by()
{
    local opts=(
        --max_memory_usage_for_user=$((150<<20))
        --max_threads=2
    )
    execute_null "${opts[@]}" <<<'SELECT uniq(number) FROM numbers_mt(toUInt64(1e6)) GROUP BY number % 5e5'
}

# This is needed to keep at least one running query for user for the time of test.
execute_null <<<'SELECT sleep(3)' &
execute_group_by
# if memory accounting will be incorrect, the second query will be failed with MEMORY_LIMIT_EXCEEDED
execute_group_by
wait
