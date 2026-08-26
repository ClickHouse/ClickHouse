#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

function wait_for_query_to_start()
{
    while [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$1'") == 0 ]]; do sleep 0.1; done
}

# Run a test query that takes very long to run.
query_id="01572_kill_window_function-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "SELECT sum(number) OVER (PARTITION BY number % 10 ORDER BY number DESC NULLS FIRST ROWS BETWEEN CURRENT ROW AND 99999 FOLLOWING) FROM numbers(0, 10000000) format Null;" >/dev/null 2>&1 &
client_pid=$!
echo Started

wait_for_query_to_start $query_id

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

