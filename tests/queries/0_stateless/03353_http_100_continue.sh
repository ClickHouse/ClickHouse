#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

checkForMissing100ContinueResponse() {
    if echo "$result" | grep -q "HTTP/1.1 100 Continue"; then
        echo "✗ Unexpected 100 Continue response"
        echo "$result"
    else
        echo "✓ No 100 Continue response (expected)"
    fi
}

checkFor100ContinueResponse() {
    if echo "$result" | grep -q "HTTP/1.1 100 Continue"; then
        echo "✓ Received 100 Continue response"
    else
        echo "✗ Missing 100 Continue response"
        echo "$result"
    fi
}

checkFor200OkResponse() {
    if echo "$result" | grep -q "HTTP/1.1 200 OK"; then
        echo "✓ Received 200 OK response"
    else
        echo "✗ Missing 200 OK response"
        echo "$result"
    fi
}

checkForTimeout() {
    if echo "$result" | grep -q "Operation timed out"; then
        echo "✓ Timed out (expected)"
    else
        echo "✗ Missing timeout"
        echo "$result"
    fi
}

checkForMissingBodyUpload() {
    if echo "$result" | grep -q "We are completely uploaded and fine"; then
        echo "✗ Unexpected body upload"
        echo "$result"
    else
        echo "✓ No body uploaded (expected)"
    fi
}

checkForError() {
    if echo "$result" | grep -q "$1"; then
        echo "✓ Received $1"
    else
        echo "✗ Missing $1"
        echo "$result"
    fi
}

# Note: The combination of expect100-timeout and max-time (overall timeout) for curl is there to ensure that we fail with a timeout if 
# we don't receive a 100 Continue response, instead of silently continuing with sending the body, which could hide bugs.

echo "=== Test: Basic Expect 100-Continue (not deferred) ==="
result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H "Expect: 100-continue" --expect100-timeout 300 --max-time 60 -d "SELECT 1" 2>&1)
checkFor100ContinueResponse
checkFor200OkResponse

echo "=== Test: Without Expect header ==="
result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -d "SELECT 1" 2>&1)
checkForMissing100ContinueResponse
checkFor200OkResponse

echo "=== Test: Deferred 100 Continue (query in body) ==="
result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H "Expect: 100-continue" -H "X-ClickHouse-100-Continue: defer" --expect100-timeout 300 --max-time 5 -d "SELECT 1" 2>&1)
checkForMissing100ContinueResponse
checkForTimeout

echo "=== Test: Deferred 100 Continue (query in URL) ==="
result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&query=SELECT%201" -H "Expect: 100-continue" -H "X-ClickHouse-100-Continue: defer" --expect100-timeout 300 --max-time 60 2>&1)
checkFor100ContinueResponse
checkFor200OkResponse

echo "=== Test: Deferred 100 Continue (INSERT data in body) ==="
$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS expect_100_continue;
CREATE OR REPLACE TABLE expect_100_continue (a UInt8) ENGINE = Memory;
"
result=$(echo -ne '10\n11\n12\n' | ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20expect_100_continue%20FORMAT%20TabSeparated" -H "Expect: 100-continue" -H "X-ClickHouse-100-Continue: defer" --expect100-timeout 300 --max-time 60 -d @- 2>&1)
checkFor100ContinueResponse
checkFor200OkResponse

echo "=== Test: Deferred 100 Continue with TOO_MANY_SIMULTANEOUS_QUERIES ==="
$CLICKHOUSE_CLIENT --query_id "sleep_${CLICKHOUSE_TEST_UNIQUE_NAME}" --query 'SELECT sleep(300) SETTINGS function_sleep_max_microseconds_per_block=300000000' >/dev/null 2>&1 &
while true
do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.processes WHERE query_id = 'sleep_${CLICKHOUSE_TEST_UNIQUE_NAME}'" | grep -F '1' >/dev/null && break || sleep 1
done
result=$(echo -ne '10\n11\n12\n' | ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20expect_100_continue%20FORMAT%20TabSeparated&max_concurrent_queries_for_all_users=1" -H "Expect: 100-continue" -H "X-ClickHouse-100-Continue: defer" --expect100-timeout 300 --max-time 60 -d @- 2>&1)
checkForMissing100ContinueResponse
checkForMissingBodyUpload
checkForError TOO_MANY_SIMULTANEOUS_QUERIES
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = 'sleep_${CLICKHOUSE_TEST_UNIQUE_NAME}' SYNC" >/dev/null
wait

echo "=== Test: Deferred 100 Continue with AUTHENTICATION_FAILED ==="
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
result=$(echo -ne '10\n11\n12\n' | ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20expect_100_continue%20FORMAT%20TabSeparated&user=user_${CLICKHOUSE_TEST_UNIQUE_NAME}" -H "Expect: 100-continue" -H "X-ClickHouse-100-Continue: defer" --expect100-timeout 300 --max-time 60 -d @- 2>&1)
checkForMissing100ContinueResponse
checkForMissingBodyUpload
checkForError AUTHENTICATION_FAILED

echo "=== Test: Deferred 100 Continue with QUOTA_EXCEEDED ==="
$CLICKHOUSE_CLIENT -n --query "
CREATE USER user_${CLICKHOUSE_TEST_UNIQUE_NAME};
GRANT INSERT ON expect_100_continue TO user_${CLICKHOUSE_TEST_UNIQUE_NAME};
DROP QUOTA IF EXISTS quota_${CLICKHOUSE_TEST_UNIQUE_NAME};
CREATE QUOTA quota_${CLICKHOUSE_TEST_UNIQUE_NAME} KEYED BY user_name FOR INTERVAL 1 month MAX query_inserts = 1 TO user_${CLICKHOUSE_TEST_UNIQUE_NAME};
"
# use up quota
$CLICKHOUSE_CLIENT --user "user_${CLICKHOUSE_TEST_UNIQUE_NAME}" --query "INSERT INTO expect_100_continue VALUES (1)" >/dev/null
result=$(echo -ne '10\n11\n12\n' | ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20expect_100_continue%20FORMAT%20TabSeparated&user=user_${CLICKHOUSE_TEST_UNIQUE_NAME}" -H "Expect: 100-continue" -H "X-ClickHouse-100-Continue: defer" --expect100-timeout 300 --max-time 60 -d @- 2>&1)
checkForMissing100ContinueResponse
checkForMissingBodyUpload
checkForError QUOTA_EXCEEDED
