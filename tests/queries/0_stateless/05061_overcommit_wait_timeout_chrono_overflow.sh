#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: each burst needs a few seconds of contention to overcommit, like 02294_overcommit_overflow

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_NAME="u05061_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $USER_NAME"
$CLICKHOUSE_CLIENT -q "CREATE USER $USER_NAME IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO $USER_NAME"

# Overcommit only waits when a query is overcommitted against another query of the same user, so the
# burst needs several queries competing for one small user-level limit. A wait this far in the future
# ends only when another query of the same user releases memory, so every query also carries an
# execution deadline to keep the burst bounded.
function burst()
{
    local wait_microseconds=$1
    for _ in {1..10}
    do
        (
            local time_limit=$((SECONDS+3))
            while [ $SECONDS -lt "$time_limit" ]
            do
                $CLICKHOUSE_CLIENT -u "$USER_NAME" -q "
                    SELECT number FROM numbers(130000) GROUP BY number
                    SETTINGS max_memory_usage_for_user = 5000000,
                             memory_overcommit_ratio_denominator = 2000000000000000000,
                             memory_usage_overcommit_max_wait_microseconds = $wait_microseconds,
                             max_execution_time = 3
                " >/dev/null 2>/dev/null
            done
        ) &
    done
    wait
}

# In range, so the wait is expected to expire on its own.
burst 500
# The smallest wait whose conversion to nanoseconds does not fit into a signed 64-bit count.
burst 9223372036854776
# Above the signed range, where an unsigned setting used to arrive as an already-expired wait.
burst 18446744073709551615

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $USER_NAME"
$CLICKHOUSE_CLIENT -q "SELECT 1"
