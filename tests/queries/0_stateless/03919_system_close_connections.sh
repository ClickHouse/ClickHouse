#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Note: commands that close TCP connections must be sent via HTTP,
# otherwise the command closes its own connection before the response is delivered.

# Cleanup: ensure TCP listen is restored even if the test fails
function cleanup()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id LIKE 'close_conn_%_${CLICKHOUSE_DATABASE}_%' SYNC" > /dev/null 2>&1 || true
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM START LISTEN TCP" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# Helper: wait for a query to appear in system.processes (via HTTP)
function wait_query_started()
{
    local query_id="$1"
    local timeout=10
    local start=$EPOCHSECONDS
    while [[ $(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query_id='$query_id'") == 0 ]]; do
        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout waiting for query $query_id to start"
            return 1
        fi
        sleep 0.1
    done
}

# --- Test 1: SYSTEM CLOSE CONNECTIONS TCP disconnects a client ---

sleep_query_id="close_conn_test_${CLICKHOUSE_DATABASE}_$RANDOM"

# Start a long-running TCP query
timeout 15s ${CLICKHOUSE_CLIENT} --query_id="$sleep_query_id" --function_sleep_max_microseconds_per_block="1000000000" \
    --query "SELECT sleep(1000)" >/dev/null 2>&1 &
client_pid=$!

wait_query_started "$sleep_query_id"

# Close all TCP connections via HTTP
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM CLOSE CONNECTIONS TCP" > /dev/null

# The client process should exit because its TCP connection was shut down
# Use timeout to avoid hanging if it doesn't exit
timeout 10s bash -c "wait $client_pid" 2>/dev/null
client_exit_code=$?
if [[ $client_exit_code -ne 0 ]]; then
    echo "client disconnected"
else
    echo "ERROR: client was not disconnected (exit code: $client_exit_code)"
fi

# Clean up server-side query
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id='$sleep_query_id' SYNC" > /dev/null 2>&1 || true

# Verify server still accepts new TCP connections (listen was NOT stopped)
${CLICKHOUSE_CLIENT} --query "SELECT 'server still accepts connections'"

# --- Test 2: SYSTEM CLOSE CONNECTIONS AND STOP LISTEN TCP ---

sleep_query_id2="close_conn_stop_test_${CLICKHOUSE_DATABASE}_$RANDOM"

timeout 15s ${CLICKHOUSE_CLIENT} --query_id="$sleep_query_id2" --function_sleep_max_microseconds_per_block="1000000000" \
    --query "SELECT sleep(1000)" >/dev/null 2>&1 &
client_pid2=$!

wait_query_started "$sleep_query_id2"

# Close connections AND stop listen via HTTP
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM CLOSE CONNECTIONS AND STOP LISTEN TCP" > /dev/null

# The client process should be disconnected
timeout 10s bash -c "wait $client_pid2" 2>/dev/null
client_exit_code2=$?
if [[ $client_exit_code2 -ne 0 ]]; then
    echo "client disconnected and listen stopped"
else
    echo "ERROR: client was not disconnected (exit code: $client_exit_code2)"
fi

# Clean up server-side query
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id='$sleep_query_id2' SYNC" > /dev/null 2>&1 || true

# New TCP connections should fail (listen is stopped)
${CLICKHOUSE_CLIENT} --connect_timeout 2 --query "SELECT 1" 2>&1 | grep -c -E "Connection refused|NETWORK_ERROR" || true

# Restore TCP listening via HTTP (also done in cleanup trap)
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM START LISTEN TCP" > /dev/null 2>&1
sleep 1

# Verify TCP is restored
${CLICKHOUSE_CLIENT} --query "SELECT 'tcp restored'"
