#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TEST_PREFIX="${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "drop user if exists u_00600${TEST_PREFIX}"
${CLICKHOUSE_CLIENT} -q "create user u_00600${TEST_PREFIX} settings max_execution_time=60, readonly=1, max_rows_to_read=0"
${CLICKHOUSE_CLIENT} -q "grant select on system.numbers to u_00600${TEST_PREFIX}"

function wait_for_query_to_start()
{
    while [[ 0 -eq $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$1'") ]]
    do
        sleep 0.1
    done
}

function wait_for_queries_to_finish()
{
    while [[ 0 -ne $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE current_database = '${CLICKHOUSE_DATABASE}' AND query NOT LIKE '%this query%'") ]]
    do
        sleep 0.1
    done
}


$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query_id=${CLICKHOUSE_DATABASE}hello&replace_running_query=1&max_rows_to_read=0" -d 'SELECT 1, count() FROM system.numbers' > /dev/null 2>&1 &
wait_for_query_to_start "${CLICKHOUSE_DATABASE}hello"

# Replace it
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query_id=${CLICKHOUSE_DATABASE}hello&replace_running_query=1" -d 'SELECT 0'

# Wait for it to be replaced
wait
wait_for_queries_to_finish

${CLICKHOUSE_CLIENT_BINARY} --user=u_00600${TEST_PREFIX} --query_id="${CLICKHOUSE_DATABASE}42" --query='SELECT 2, count() FROM system.numbers' 2>&1 | grep -cF 'QUERY_WAS_CANCELLED' &
wait_for_query_to_start "${CLICKHOUSE_DATABASE}42"

# Trying to run another query with the same query_id
${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_DATABASE}42" --query='SELECT 43' 2>&1 | grep -cF 'is already running by user'

# Trying to replace query of a different user
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query_id=${CLICKHOUSE_DATABASE}42&replace_running_query=1" -d 'SELECT 1' | grep -cF 'is already running by user'

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '${CLICKHOUSE_DATABASE}42' SYNC" > /dev/null
wait
wait_for_queries_to_finish

${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_DATABASE}42" --max_rows_to_read=0 --query='SELECT 3, count() FROM system.numbers' 2>&1 | grep -cF 'QUERY_WAS_CANCELLED' &
wait_for_query_to_start "${CLICKHOUSE_DATABASE}42"
${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_DATABASE}42" --replace_running_query=1 --replace_running_query_max_wait_ms=500 --query='SELECT 43' 2>&1 | grep -F "can't be stopped" > /dev/null
wait
wait_for_queries_to_finish

${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_DATABASE}42" --replace_running_query=1 --query='SELECT 44'
${CLICKHOUSE_CLIENT} -q "drop user u_00600${TEST_PREFIX}"
