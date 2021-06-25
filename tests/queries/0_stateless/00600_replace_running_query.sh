#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh


function wait_for_query_to_start()
{
    while [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$1'") == 0 ]]; do sleep 0.1; done
}


$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query_id=hello&replace_running_query=1" -d 'SELECT 1, count() FROM system.numbers' > /dev/null 2>&1 &
wait_for_query_to_start 'hello'

# Replace it
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query_id=hello&replace_running_query=1" -d 'SELECT 0'

# Wait for it to be replaced
wait

${CLICKHOUSE_CLIENT_BINARY} --user=readonly --query_id=42 --query='SELECT 2, count() FROM system.numbers' 2>&1 | grep -cF 'was cancelled' &
wait_for_query_to_start '42'

# Trying to run another query with the same query_id
${CLICKHOUSE_CLIENT} --query_id=42 --query='SELECT 43' 2>&1 | grep -cF 'is already running by user'

# Trying to replace query of a different user
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query_id=42&replace_running_query=1" -d 'SELECT 1' | grep -cF 'is already running by user'

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '42' SYNC" > /dev/null
wait

${CLICKHOUSE_CLIENT} --query_id=42 --query='SELECT 3, count() FROM system.numbers' 2>&1 | grep -cF 'was cancelled' &
wait_for_query_to_start '42'
${CLICKHOUSE_CLIENT} --query_id=42 --replace_running_query=1 --replace_running_query_max_wait_ms=500 --query='SELECT 43' 2>&1 | grep -F "can't be stopped" > /dev/null
wait
${CLICKHOUSE_CLIENT} --query_id=42 --replace_running_query=1 --query='SELECT 44'
