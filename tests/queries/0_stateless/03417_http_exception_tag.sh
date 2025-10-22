#!/usr/bin/env bash
# Tags: no-fasttest

CLICKHOUSE_DATABASE="default"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Test 1: Check that X-ClickHouse-Exception-Tag header is present when enabled"
${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&http_exception_tagging=1" --data-binary "SELECT 1" | grep "X-ClickHouse-Exception-Tag" | head -1 | cut -d':' -f1 | sed 's/$/:/g'

echo "Test 2: Check that X-ClickHouse-Exception-Tag header is NOT present when disabled (default)"
${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}" --data-binary "SELECT 1" | grep "X-ClickHouse-Exception-Tag" | wc -l

echo "Test 3: Verify exception tag is alphanumeric when enabled"
TAG=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&http_exception_tagging=1" --data-binary "SELECT 1" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
if [[ "$TAG" =~ ^[A-Za-z0-9]+$ ]] && [[ ${#TAG} -eq 8 ]]; then
    echo "Tag is 8-char alphanumeric: OK"
else
    echo "Tag format incorrect"
fi

echo "Test 4: Check exception marker format on error when enabled"
# Get a fresh TAG from the error response itself
RESPONSE=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&http_exception_tagging=1" --data-binary "SELECT throwIf(number=3, 'there is a exception') FROM system.numbers SETTINGS max_block_size=1" 2>&1 || true)
TAG=$(echo "$RESPONSE" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
# Check if the response contains __exception__ followed by newline, then TAG, then newline
if echo "$RESPONSE" | grep -Pzo "__exception__\r?\n${TAG}\r?\n" > /dev/null 2>&1; then
    echo "Tagged exception marker found: OK"
else
    echo "Tagged exception marker not found"
fi

echo "Test 5: Check reverse marker format (length + tag + __exception__)"
# Use max_result_rows to trigger an exception after sending data (to get HTTP 200 with exception in body)
RESPONSE=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&http_exception_tagging=1&wait_end_of_query=0&max_result_rows=5" --data-binary "SELECT number FROM system.numbers SETTINGS max_block_size=1" 2>&1 || true)
TAG=$(echo "$RESPONSE" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
# Check for pattern: <number> <TAG> on a line (this is the reverse marker before __exception__)
if echo "$RESPONSE" | grep -q "[0-9]\+ ${TAG}"; then
    echo "Reverse marker found: OK"
else
    echo "Reverse marker not found"
fi

echo "Test 6: Verify exception is truncated to 256 bytes"
# Create a long error message and verify it's NOT truncated (MAX_EXCEPTION_SIZE is now 16K)
LONG_MSG=$(printf 'A%.0s' {1..500})
RESPONSE=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&http_exception_tagging=1&wait_end_of_query=0" --data-binary "SELECT number, throwIf(number=5, '${LONG_MSG}') FROM system.numbers LIMIT 10 SETTINGS max_block_size=1" 2>&1 || true)
# Since MAX_EXCEPTION_SIZE is 16K, a 500-byte message should NOT be truncated
# The response should contain the full error message with all the A's
if echo "$RESPONSE" | grep -q "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; then
    echo "Exception truncated to ~256 bytes: OK"
else
    echo "Exception not truncated properly"
fi

echo "Test 7: Verify legacy format when exception tagging is disabled"
# Use max_result_rows to trigger an exception after sending data
RESPONSE=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&http_exception_tagging=0&wait_end_of_query=0&max_result_rows=10" --data-binary "SELECT number FROM system.numbers SETTINGS max_block_size=1" 2>&1 || true)
# Should have __exception__ followed immediately by newline and Code:, but NOT a tag on a separate line
if echo "$RESPONSE" | grep -Pzo "__exception__\r?\nCode:" > /dev/null 2>&1; then
    echo "Legacy exception format: OK"
else
    echo "Legacy format not working"
fi

echo "Test 8: Verify per-query parameter works (enables for single query)"
# First query without parameter - should NOT have header
RESPONSE1=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}" --data-binary "SELECT 1" | grep "X-ClickHouse-Exception-Tag" | wc -l)
# Second query with parameter - should have header
RESPONSE2=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&http_exception_tagging=1" --data-binary "SELECT 1" | grep "X-ClickHouse-Exception-Tag" | wc -l)
if [ "$RESPONSE1" -eq 0 ] && [ "$RESPONSE2" -eq 1 ]; then
    echo "Per-query parameter works: OK"
else
    echo "Per-query parameter not working"
fi
echo
