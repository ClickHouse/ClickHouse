#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel, no-fasttest, no-tsan, no-asan
# Tag no-fasttest: max_memory_usage_for_user can interfere another queries running concurrently

# Regression for MemoryTracker that had been incorrectly accounted
# (it was reset before deallocation)
#
# For this will be used:
# - two-level group by
# - max_memory_usage_for_user
# - one users' query in background (to avoid reseting max_memory_usage_for_user)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

function execute_null()
{
    ${CLICKHOUSE_CLIENT} --format Null -n "$@"
}

function execute_group_by()
{
    # Peak memory usage for the main query (with GROUP BY) is ~100MiB (with
    # max_threads=2 as here).
    # So set max_memory_usage_for_user to 150MiB and if the memory tracking
    # accounting will be incorrect then the second query will fail
    #
    # Note that we also need one running query for the user (sleep(3)), since
    # max_memory_usage_for_user is installed to 0 once there are no more
    # queries for user.
    local opts=(
        "--max_memory_usage_for_user="$((150<<20))
        "--max_threads=2"
    )
    execute_null "${opts[@]}" <<<'SELECT uniq(number) FROM numbers_mt(1e6) GROUP BY number % 5e5'
}

# This is needed to keep at least one running query for user for the time of test.
execute_null <<<'SELECT sleep(3)' &
execute_group_by
# if memory accounting will be incorrect, the second query will be failed with MEMORY_LIMIT_EXCEEDED
execute_group_by
wait

# Reset max_memory_usage_for_user, so it will not affect other tests
${CLICKHOUSE_CLIENT} --max_memory_usage_for_user=0 -q "SELECT 1 FORMAT Null"
