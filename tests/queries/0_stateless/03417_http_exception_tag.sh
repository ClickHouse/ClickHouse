#!/usr/bin/env bash
# Tags: no-fasttest

set -x

# NOTE: we use throwIf(number=6000000) to make sure server sends at least one block (default max_size is 65409 and some tests we explicitly test ~5M) before failing to send exception in mid-stream
CLICKHOUSE_DATABASE="default"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# echo "Test 1: Check that X-ClickHouse-Exception-Tag header is present"
# ${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}" --data-binary "SELECT throwIf(number=6000000, 'there is a exception') FROM system.numbers LIMIT 60000010" 2>&1 | grep "X-ClickHouse-Exception-Tag" | head -1 | cut -d':' -f1 | sed 's/$/:/g'
#
# echo "Test 2: Verify exception tag is alphanumeric"
# TAG=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}" --data-binary "SELECT throwIf(number=6000000, 'there is a exception') FROM system.numbers LIMIT 60000010" 2>&1 | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
# if [[ "$TAG" =~ ^[A-Za-z0-9]+$ ]] && [[ ${#TAG} -eq 8 ]]; then
#     echo "Tag is 8-char alphanumeric: OK"
# else
#     echo "Tag format incorrect"
# fi
#
# echo "Test 3: Check exception marker format on error"
# # Get a fresh TAG from the error response itself
# RESPONSE=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}" --data-binary "SELECT throwIf(number=6000000, 'there is a exception') FROM system.numbers LIMIT 60000010" 2>&1 || true)
# TAG=$(echo "$RESPONSE" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
# # Check if the response contains __exception__ followed by newline, then TAG, then newline
# if echo "$RESPONSE" | grep -Pzo "__exception__\r?\n${TAG}\r?\n" > /dev/null 2>&1; then
#     echo "Tagged exception marker found: OK"
# else
#     echo "Tagged exception marker not found"
# fi

echo "Test 4: Check reverse marker format (length + tag + __exception__)"
# Use a query that deterministically sends some data first, then throws
RESPONSE=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}" --data-binary "SELECT throwIf(number=6000000, 'there is a exception') FROM system.numbers LIMIT 60000010" 2>&1 || true)
TAG=$(echo "$RESPONSE" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
# Check for pattern: <number> <TAG> on a line (this is the reverse marker before __exception__)
if echo "$RESPONSE" | grep -q "[0-9]\+ ${TAG}"; then
    echo "Reverse marker found: OK"
else
    echo "Reverse marker not found"
fi
#
# echo "Test 5: Verify exception message handling"
# # Create a long error message and verify it's NOT truncated (MAX_EXCEPTION_SIZE is now 16K)
# LONG_MSG=$(printf 'A%.0s' {1..500})
# RESPONSE=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}" --data-binary "SELECT throwIf(number=6000000, '${LONG_MSG}') FROM system.numbers LIMIT 60000010" 2>&1 || true)
# # Since MAX_EXCEPTION_SIZE is 16K, a 500-byte message should NOT be truncated
# # The response should contain the full error message with all the A's
# if echo "$RESPONSE" | grep -q "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; then
#     echo "Exception message handling: OK"
# else
#     echo "Exception message handling failed"
# fi
# echo
