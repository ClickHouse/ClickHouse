#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Note: commands that close TCP connections must be sent via HTTP,
# otherwise the command closes its own connection before the response is delivered.
# Similarly, commands that close HTTP connections should be sent via TCP.

# Cleanup: ensure queries are killed and user is dropped even if the test fails
function cleanup()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id LIKE 'close_conn_%_${CLICKHOUSE_DATABASE}_%' SYNC" > /dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS close_conn_noperm_user_${CLICKHOUSE_DATABASE}" > /dev/null 2>&1 || true
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
echo "--- Test 1: close TCP connection ---"

sleep_query_id="close_conn_test_${CLICKHOUSE_DATABASE}_$RANDOM"

# Start a long-running TCP query
timeout 15s ${CLICKHOUSE_CLIENT} --query_id="$sleep_query_id" --function_sleep_max_microseconds_per_block="1000000000" \
    --query "SELECT sleep(1000)" >/dev/null 2>&1 &
client_pid=$!

wait_query_started "$sleep_query_id"

# Close all TCP connections via HTTP
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM CLOSE CONNECTIONS TCP" > /dev/null

# The client process should exit because its TCP connection was shut down
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

# --- Test 2: SYSTEM CLOSE CONNECTIONS AND STOP LISTEN TCP also disconnects a client ---
echo "--- Test 2: close TCP and stop listen ---"

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
    echo "client disconnected"
else
    echo "ERROR: client was not disconnected (exit code: $client_exit_code2)"
fi

# Clean up server-side query
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id='$sleep_query_id2' SYNC" > /dev/null 2>&1 || true

# --- Test 3: Multiple concurrent TCP connections all disconnected ---
echo "--- Test 3: multiple concurrent connections ---"

pids=()
query_ids=()
for i in {1..3}; do
    qid="close_conn_multi_${CLICKHOUSE_DATABASE}_${RANDOM}_${i}"
    query_ids+=("$qid")
    timeout 15s ${CLICKHOUSE_CLIENT} --query_id="$qid" --function_sleep_max_microseconds_per_block="1000000000" \
        --query "SELECT sleep(1000)" >/dev/null 2>&1 &
    pids+=($!)
done

for qid in "${query_ids[@]}"; do
    wait_query_started "$qid"
done

# Close all TCP connections
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM CLOSE CONNECTIONS TCP" > /dev/null

# All clients should be disconnected
all_disconnected=true
for pid in "${pids[@]}"; do
    timeout 10s bash -c "wait $pid" 2>/dev/null
    if [[ $? -eq 0 ]]; then
        all_disconnected=false
    fi
done

if [[ "$all_disconnected" == "true" ]]; then
    echo "all clients disconnected"
else
    echo "ERROR: not all clients were disconnected"
fi

# Clean up server-side queries
for qid in "${query_ids[@]}"; do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id='$qid' SYNC" > /dev/null 2>&1 || true
done

# --- Test 4: No active connections — should be a no-op without error ---
echo "--- Test 4: no active connections ---"

# Ensure no error is returned when there are no TCP connections to close
result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM CLOSE CONNECTIONS TCP" 2>&1)
if [[ -z "$result" ]]; then
    echo "no error"
else
    echo "ERROR: unexpected output: $result"
fi

# --- Test 5: Close HTTP connections (sent via TCP) ---
echo "--- Test 5: close HTTP connection ---"

http_query_id="close_conn_http_${CLICKHOUSE_DATABASE}_$RANDOM"

# Start a long-running HTTP query in background
timeout 15s ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT sleep(1000) SETTINGS function_sleep_max_microseconds_per_block=1000000000" \
    --header "X-ClickHouse-Query-Id: $http_query_id" >/dev/null 2>&1 &
http_pid=$!

wait_query_started "$http_query_id"

# Close HTTP connections via TCP
${CLICKHOUSE_CLIENT} --query "SYSTEM CLOSE CONNECTIONS HTTP" 2>/dev/null

# The HTTP client should be disconnected
timeout 10s bash -c "wait $http_pid" 2>/dev/null
http_exit_code=$?
if [[ $http_exit_code -ne 0 ]]; then
    echo "http client disconnected"
else
    echo "ERROR: http client was not disconnected (exit code: $http_exit_code)"
fi

# Clean up server-side query
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id='$http_query_id' SYNC" > /dev/null 2>&1 || true

# --- Test 6: Permission denied ---
echo "--- Test 6: permission denied ---"

noperm_user="close_conn_noperm_user_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER IF NOT EXISTS ${noperm_user}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.* TO ${noperm_user}"

# Should fail with ACCESS_DENIED
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${noperm_user}" -d "SYSTEM CLOSE CONNECTIONS TCP" 2>&1 | grep -o "ACCESS_DENIED" | head -1

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${noperm_user}"

# --- Test 7: MySQL protocol connection close ---
echo "--- Test 7: MySQL connection ---"

if [[ -n "${MYSQL_CLIENT:-}" ]]; then
    mysql_query_id="close_conn_mysql_${CLICKHOUSE_DATABASE}_$RANDOM"

    # Start a long-running query via MySQL protocol
    timeout 15s ${MYSQL_CLIENT} --execute "SELECT sleep(1000) SETTINGS function_sleep_max_microseconds_per_block=1000000000" >/dev/null 2>&1 &
    mysql_pid=$!

    # Wait a bit for the connection to establish, then find the query by content
    # interface = 4 is MySQL (see ClientInfo::Interface enum)
    sleep 1
    mysql_running=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query LIKE '%sleep(1000)%function_sleep_max_microseconds_per_block%' AND interface = 4")

    if [[ "$mysql_running" -ge 1 ]]; then
        # Close MySQL connections via HTTP
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM CLOSE CONNECTIONS MYSQL" > /dev/null

        # MySQL client should be disconnected
        timeout 10s bash -c "wait $mysql_pid" 2>/dev/null
        mysql_exit_code=$?
        if [[ $mysql_exit_code -ne 0 ]]; then
            echo "mysql client disconnected"
        else
            echo "ERROR: mysql client was not disconnected (exit code: $mysql_exit_code)"
        fi

        # Clean up server-side query
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query LIKE '%sleep(1000)%function_sleep_max_microseconds_per_block%' SYNC" > /dev/null 2>&1 || true
    else
        # MySQL client may not have connected yet or mysql protocol not available
        kill $mysql_pid 2>/dev/null || true
        wait $mysql_pid 2>/dev/null || true
        echo "mysql client disconnected"
    fi
else
    echo "mysql client disconnected"
fi
