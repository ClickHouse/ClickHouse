#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

# Run a test query that takes very long to run.
query_id="01572_kill_window_function-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "SELECT count(1048575) OVER (PARTITION BY intDiv(NULL, number) ORDER BY number DESC NULLS FIRST ROWS BETWEEN CURRENT ROW AND 1048575 FOLLOWING) FROM numbers(255, 1048575)" >/dev/null 2>&1 &
client_pid=$!
echo Started

# Use one query to both kill the test query and verify that it has started,
# because if we try to kill it before it starts, the test will fail.
while [ -z "$($CLICKHOUSE_CLIENT --query "kill query where query_id = '$query_id' and current_database = currentDatabase()")" ]
do
    # If we don't yet see the query in the process list, the client should still
    # be running. The query is very long.
    kill -0 -- $client_pid
    sleep 1
done
echo Sent kill request

# Wait for the client to terminate.
client_exit_code=0
wait $client_pid || client_exit_code=$?

echo "Exit $client_exit_code"

# We have tested for Ctrl+C.
# The following client flags don't cancel, but should: --max_execution_time,
# --receive_timeout. Probably needs asynchonous calculation of query limits, as
# discussed with Nikolay on TG: https://t.me/c/1214350934/21492

