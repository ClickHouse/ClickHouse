#!/usr/bin/env bash
# Tags: no-parallel
# A huge low_priority_query_wait_time_ms reached condition_variable::wait_for through
# QueryPriorities::waitIfNeed and overflowed the millisecond-to-nanosecond conversion.
# The wait is now clamped, so the lower-priority query must complete once the higher-priority
# one is gone instead of aborting under UBSan.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

high_id="high_04328_$CLICKHOUSE_DATABASE"
low_id="low_04328_$CLICKHOUSE_DATABASE"

function wait_for_query_to_start()
{
    local timeout=30
    local start=$EPOCHSECONDS
    while [[ $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$1'") == 0 ]]; do
        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout waiting for query $1 to start" >&2
            exit 1
        fi
        sleep 0.1
    done
}

# Higher-priority query keeps running so the lower-priority one has to wait.
${CLICKHOUSE_CLIENT} --query_id "$high_id" --query "SELECT count() FROM numbers(100000000000) SETTINGS priority = 1, max_threads = 1, max_block_size = 65505, max_rows_to_read = 0" > /dev/null 2>&1 &
wait_for_query_to_start "$high_id"

# Lower-priority query waits with a timeout big enough to overflow ms->ns before the clamp.
${CLICKHOUSE_CLIENT} --query_id "$low_id" --query "SELECT count() FROM numbers(100000000) SETTINGS priority = 2, low_priority_query_wait_time_ms = 100000000000000, max_threads = 1, max_block_size = 65505, max_rows_to_read = 0" > /dev/null 2>&1 &
wait_for_query_to_start "$low_id"

# Releasing the higher-priority query lets the lower-priority one finish.
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$high_id' SYNC" > /dev/null
wait

echo "OK"
