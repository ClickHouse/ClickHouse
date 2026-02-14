#!/usr/bin/env bash
# Test detach_non_readonly_queries for HTTP and native protocol: when enabled, non-readonly queries
# (e.g. INSERT, INSERT...SELECT) are detached — server returns immediately with query_id while the
# query runs in a background thread. Covers both HTTP interface and native (clickhouse-client).
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Set before sourcing shell_config so CLICKHOUSE_CLIENT etc. use the build binary
export CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-${CURDIR}/../../../build/programs/clickhouse}"
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="detach_test_$$"
# Unique query_id per run to avoid QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING when tests run in parallel (e.g. stress)
QUERY_ID_PREFIX="03812_$$_${RANDOM}_"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TABLE (x UInt64) ENGINE=MergeTree() ORDER BY x"

# --- HTTP interface ---
# 1. With detach_non_readonly_queries=0 (default), INSERT-SELECT runs synchronously.
echo "=== HTTP: Sync mode (default): INSERT-SELECT waits for completion ==="
BODY_SYNC=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+$TABLE+SELECT+1" -X POST -d "" 2>/dev/null || true)
HEADER_SYNC=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&query=INSERT+INTO+$TABLE+SELECT+2" -X POST -d "" -o /dev/null 2>/dev/null | grep -i "X-ClickHouse-Query-Id" || true)
echo "Sync response body length: ${#BODY_SYNC}"
echo 'Sync has X-ClickHouse-Query-Id header: '"$( [ -n "$HEADER_SYNC" ] && echo yes || echo no )"

# 2. With detach_non_readonly_queries=1, INSERT-SELECT returns immediately with query_id in body and header.
echo "=== HTTP: Detach non-readonly mode: INSERT-SELECT returns immediately with query_id ==="
QUERY_ID_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&detach_non_readonly_queries=1&query_id=${QUERY_ID_PREFIX}h1&query=INSERT+INTO+$TABLE+SELECT+3" -X POST -d "" 2>/dev/null || true)
echo "Detach non-readonly response body (query_id): $([ -n "$QUERY_ID_HTTP" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$QUERY_ID_HTTP" ]; then
    echo "FAIL: Expected non-empty query_id in response body when detach_non_readonly_queries=1"
    exit 1
fi

sleep 30

# 3. SELECT (readonly) is never detached.
echo "=== HTTP: SELECT remains synchronous even with detach_non_readonly_queries=1 ==="
SELECT_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&detach_non_readonly_queries=1&query=SELECT+count()+FROM+$TABLE" -X POST -d "" 2>/dev/null)
echo "SELECT result: $SELECT_HTTP"
if [ -z "$SELECT_HTTP" ]; then
    echo "FAIL: SELECT should return result synchronously"
    exit 1
fi

# 4. Verify data (1, 2 sync + 3 detach = 3 rows).
COUNT_HTTP=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE" 2>/dev/null)
echo "Rows in table after HTTP detached inserts: $COUNT_HTTP"
if [ "$COUNT_HTTP" -lt 3 ]; then
    echo "FAIL: Expected at least 3 rows (sync 1,2 and detach 3), got $COUNT_HTTP"
    exit 1
fi

# 5. INSERT with inline data (query in URL, data in body) in detach mode returns query_id.
echo "=== HTTP: Detach non-readonly INSERT with body data returns query_id ==="
ASYNC_INSERT_BODY=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&detach_non_readonly_queries=1&query_id=${QUERY_ID_PREFIX}h2&query=INSERT+INTO+$TABLE+FORMAT+TabSeparated" -X POST -d "4" 2>/dev/null)
echo "Detach non-readonly INSERT with body response: $([ -n "$ASYNC_INSERT_BODY" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$ASYNC_INSERT_BODY" ]; then
    echo "FAIL: Expected query_id in response for detached INSERT with body"
    exit 1
fi

# 6. Query in POST body only (no ?query= in URL).
echo "=== HTTP: Detach non-readonly with query in POST body only (no query= param) ==="
ASYNC_POST_BODY_ONLY=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&detach_non_readonly_queries=1&query_id=${QUERY_ID_PREFIX}h3" -X POST --data-binary "INSERT INTO $TABLE SELECT 5" 2>/dev/null)
echo "Detach non-readonly POST body-only response (query_id): $([ -n "$ASYNC_POST_BODY_ONLY" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$ASYNC_POST_BODY_ONLY" ]; then
    echo "FAIL: Expected query_id when query is in POST body only"
    exit 1
fi

sleep 30
COUNT_HTTP_FINAL=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE" 2>/dev/null)
echo "HTTP final row count: $COUNT_HTTP_FINAL"
if [ "$COUNT_HTTP_FINAL" -lt 5 ]; then
    echo "FAIL: Expected at least 5 rows after HTTP section, got $COUNT_HTTP_FINAL"
    exit 1
fi

# --- Native protocol (clickhouse-client) ---
# 7. Sync INSERT.
echo "=== Native: Sync mode (default): INSERT...SELECT waits for completion ==="
$CLICKHOUSE_CLIENT -q "INSERT INTO $TABLE SELECT 6"
echo "Sync insert done"

# 8. Detach: returns immediately with query_id in result block.
echo "=== Native: Detach non-readonly mode: INSERT...SELECT returns immediately with query_id ==="
QUERY_ID_NATIVE=$($CLICKHOUSE_CLIENT --query_id "${QUERY_ID_PREFIX}native" --detach_non_readonly_queries 1 -q "INSERT INTO $TABLE SELECT 7" 2>/dev/null || true)
echo "Detach response (query_id): $([ -n "$QUERY_ID_NATIVE" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$QUERY_ID_NATIVE" ]; then
    echo "FAIL: Expected non-empty query_id in response when detach_non_readonly_queries=1"
    exit 1
fi

sleep 30

# 9. SELECT remains synchronous.
echo "=== Native: SELECT remains synchronous even with detach_non_readonly_queries=1 ==="
SELECT_NATIVE=$($CLICKHOUSE_CLIENT --detach_non_readonly_queries 1 -q "SELECT count() FROM $TABLE" 2>/dev/null)
echo "SELECT result: $SELECT_NATIVE"
if [ -z "$SELECT_NATIVE" ]; then
    echo "FAIL: SELECT should return result synchronously"
    exit 1
fi

# 10. Verify data (5 from HTTP + 6 sync + 7 detach = 7 rows).
COUNT_NATIVE=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE" 2>/dev/null)
echo "Rows in table: $COUNT_NATIVE"
if [ "$COUNT_NATIVE" -lt 7 ]; then
    echo "FAIL: Expected at least 7 rows, got $COUNT_NATIVE"
    exit 1
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE"
echo "OK"
