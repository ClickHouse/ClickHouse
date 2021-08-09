#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

# Run a test query that takes very long to run.
query_id="01572_kill_window_function"
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "SELECT count(1048575) OVER (PARTITION BY intDiv(NULL, number) ORDER BY number DESC NULLS FIRST ROWS BETWEEN CURRENT ROW AND 1048575 FOLLOWING) FROM numbers(255, 1048575)" >/dev/null 2>&1 &
client_pid=$!

# First check that clickhouse-client is still alive, and then use one query to
# both kill the test query and verify that it has started.
# If we try to kill it before it starts, the test will fail.
# If it finishes after we check it started, the test will fail.
# That's why it is better to check and kill with the same call.
while kill -0 $client_pid \
    && [ -z "$(CLICKHOUSE_CLIENT --query "kill query where query_id = '$query_id' and current_database = currentDatabase()")" ]
do
    sleep 1
done

# Wait for the client to terminate.
client_exit_code=0
wait $client_pid || client_exit_code=$?

# Note that we still have to check for normal termination, because the test query
# might have completed even before we first tried to kill it. This shouldn't
# really happen because it's very long, but our CI infractructure is known to
# introduce the most unexpected delays.
if [ $client_exit_code -eq 0 ] || [ $client_exit_code -eq 138 ]
then
    echo "OK"
else
    echo "Got unexpected client exit code $client_exit_code"
fi

# We have tested for Ctrl+C.
# The following client flags don't cancel, but should: --max_execution_time,
# --receive_timeout. Probably needs asynchonous calculation of query limits, as
# discussed with Nikolay on TG: https://t.me/c/1214350934/21492

