#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

# Wait until the query is visible in `system.processes`, with a deadline.
# The condition is "not started yet" rather than "started", so a failed request, which returns an
# empty string, keeps waiting instead of falling through to a `KILL QUERY` that matches nothing.
function wait_for_query_to_start()
{
    local deadline=$((SECONDS + 60))
    while [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$1'") != 1 ]]
    do
        if (( SECONDS >= deadline ))
        then
            echo "The query $1 has not started in 60 seconds" >&2
            return 1
        fi
        sleep 0.1
    done
}

# Run a test query that takes very long to run, but does not need much memory.
# The frame starts at the current row, so `WindowTransform` recalculates the aggregate over the
# whole frame for every row. A frame covering a single partition of a million rows makes it
# ~10^11 additions, which is about a minute and a half in a release build and longer in a
# sanitizer build, while only a million numbers are kept in memory (~20 MiB at the peak).
# The single partition is deliberate: with `PARTITION BY` the partitions are calculated in
# parallel and the frames are smaller, so the query becomes faster the higher `max_threads` is,
# and the test runner randomizes it.
query_id="01572_kill_window_function-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "SELECT sum(number) OVER (ORDER BY number DESC NULLS FIRST ROWS BETWEEN CURRENT ROW AND 999999 FOLLOWING) FROM numbers(0, 1000000) format Null;" >/dev/null 2>&1 &
client_pid=$!
echo Started

# On the early exit path the background query has to be cancelled explicitly, otherwise the test
# hangs until the runner kills the whole process group.
if ! wait_for_query_to_start "$query_id"
then
    kill "$client_pid" 2>/dev/null || true
    wait "$client_pid" 2>/dev/null || true
    exit 1
fi

$CLICKHOUSE_CLIENT --query "kill query where query_id = '$query_id' and current_database = currentDatabase() format Null"
echo Sent kill request

# Wait for the client to terminate.
client_exit_code=0
wait $client_pid || client_exit_code=$?

echo "Exit $client_exit_code"

# We have tested for Ctrl+C.
# The following client flags don't cancel, but should: --max_execution_time,
# --receive_timeout. Probably needs asynchonous calculation of query limits, as
# discussed with Nikolay on TG: https://t.me/c/1214350934/21492

