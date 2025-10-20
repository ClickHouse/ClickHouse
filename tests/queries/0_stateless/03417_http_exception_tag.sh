#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Test 1: Check that X-ClickHouse-Exception-Tag header is present when enabled"
${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}?query=SELECT+1&http_exception_tagging=1" | grep "X-ClickHouse-Exception-Tag" | head -1

echo "Test 2: Check that X-ClickHouse-Exception-Tag header is NOT present when disabled (default)"
${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}?query=SELECT+1" | grep "X-ClickHouse-Exception-Tag" | wc -l

echo "Test 3: Verify exception tag is alphanumeric when enabled"
TAG=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}?query=SELECT+1&http_exception_tagging=1" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
if [[ "$TAG" =~ ^[A-Za-z0-9]+$ ]] && [[ ${#TAG} -eq 8 ]]; then
    echo "Tag is 8-char alphanumeric: OK"
else
    echo "Tag format incorrect"
fi

echo "Test 4: Check exception marker format on error when enabled"
TAG=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}?query=SELECT+1&http_exception_tagging=1" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+throwIf(1)&http_exception_tagging=1" 2>&1 || true)
if echo "$RESPONSE" | grep -q "__exception__${TAG}"; then
    echo "Tagged exception marker found: OK"
else
    echo "Tagged exception marker not found"
fi

echo "Test 5: Check reverse marker format (length + tag + __exception__)"
TAG=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}?query=SELECT+1&http_exception_tagging=1" | grep "X-ClickHouse-Exception-Tag" | cut -d':' -f2 | tr -d ' \r\n')
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+throwIf(1)&http_exception_tagging=1" 2>&1 || true)
if echo "$RESPONSE" | grep -E "[0-9]+ ${TAG}__exception__" > /dev/null; then
    echo "Reverse marker found: OK"
else
    echo "Reverse marker not found"
fi

echo "Test 6: Verify exception is truncated to 256 bytes"
# Create a long error message and verify it's truncated
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+throwIf(1,'$(printf 'A%.0s' {1..500})')&http_exception_tagging=1" 2>&1 || true)
# Extract just the error message part (between markers)
ERROR_SIZE=$(echo "$RESPONSE" | sed -n '/__exception__/,/[0-9]* [A-Za-z0-9]*__exception__/p' | wc -c)
if [ "$ERROR_SIZE" -lt 350 ]; then
    echo "Exception truncated to ~256 bytes: OK"
else
    echo "Exception not truncated properly"
fi

echo "Test 7: Verify legacy format when exception tagging is disabled"
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+throwIf(1)&http_exception_tagging=0" 2>&1 || true)
# Should have __exception__ but NOT followed by a tag
if echo "$RESPONSE" | grep -q "__exception__" && ! echo "$RESPONSE" | grep -E "__exception__[A-Za-z0-9]{8}" > /dev/null; then
    echo "Legacy exception format: OK"
else
    echo "Legacy format not working"
fi

echo "Test 8: Verify per-query parameter works (enables for single query)"
# First query without parameter - should NOT have header
RESPONSE1=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}?query=SELECT+1" | grep "X-ClickHouse-Exception-Tag" | wc -l)
# Second query with parameter - should have header
RESPONSE2=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}?query=SELECT+1&http_exception_tagging=1" | grep "X-ClickHouse-Exception-Tag" | wc -l)
if [ "$RESPONSE1" -eq 0 ] && [ "$RESPONSE2" -eq 1 ]; then
    echo "Per-query parameter works: OK"
else
    echo "Per-query parameter not working"
fi


