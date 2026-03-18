#!/usr/bin/env bash
# Test allow_experimental_detach_non_readonly_queries for HTTP and native protocol: when enabled, non-readonly queries
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

# Wait for table to have at least expected_min rows (polls so detached inserts can finish on slow CI e.g. amd_msan).
wait_for_count() {
    local expected_min=$1
    local max_wait=${2:-90}
    local waited=0
    while [ "$waited" -lt "$max_wait" ]; do
        local c
        c=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE" 2>/dev/null || echo "0")
        if [ "${c:-0}" -ge "$expected_min" ]; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "FAIL: Waited ${max_wait}s for at least $expected_min rows, got $($CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE" 2>/dev/null || echo "?")"
    exit 1
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TABLE (x UInt64) ENGINE=MergeTree() ORDER BY x"

# --- HTTP interface ---
# 1. With allow_experimental_detach_non_readonly_queries=0 (default), INSERT-SELECT runs synchronously.
echo "=== HTTP: Sync mode (default): INSERT-SELECT waits for completion ==="
BODY_SYNC=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+$TABLE+SELECT+1" -X POST -d "" 2>/dev/null || true)
HEADER_SYNC=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&query=INSERT+INTO+$TABLE+SELECT+2" -X POST -d "" -o /dev/null 2>/dev/null | grep -i "X-ClickHouse-Query-Id" || true)
echo "Sync response body length: ${#BODY_SYNC}"
echo 'Sync has X-ClickHouse-Query-Id header: '"$( [ -n "$HEADER_SYNC" ] && echo yes || echo no )"

# 2. With allow_experimental_detach_non_readonly_queries=1, INSERT-SELECT returns immediately with query_id in body and header.
echo "=== HTTP: Detach non-readonly mode: INSERT-SELECT returns immediately with query_id ==="
QUERY_ID_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}h1&query=INSERT+INTO+$TABLE+SELECT+3" -X POST -d "" 2>/dev/null || true)
echo "Detach non-readonly response body (query_id): $([ -n "$QUERY_ID_HTTP" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$QUERY_ID_HTTP" ]; then
    echo "FAIL: Expected non-empty query_id in response body when allow_experimental_detach_non_readonly_queries=1"
    exit 1
fi

# Wait for detached INSERT (value 3) to complete (slow CI may need more than a fixed sleep).
wait_for_count 3

# 3. SELECT (readonly) is never detached.
echo "=== HTTP: SELECT remains synchronous even with allow_experimental_detach_non_readonly_queries=1 ==="
SELECT_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&query=SELECT+count()+FROM+$TABLE" -X POST -d "" 2>/dev/null)
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

# 5. INSERT with body data (query in URL, data in body) is intentionally NOT detached (payload-driven inserts run sync).
echo "=== HTTP: INSERT with body data runs synchronously (not detached) ==="
INSERT_BODY_HTTP_CODE=$(${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}" "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}h2&query=INSERT+INTO+$TABLE+FORMAT+TabSeparated" -X POST -d "4" 2>/dev/null || echo "000")
echo "INSERT with body HTTP code: $INSERT_BODY_HTTP_CODE"
if [ "$INSERT_BODY_HTTP_CODE" != "200" ]; then
    echo "FAIL: Expected HTTP 200 for INSERT with body (sync), got $INSERT_BODY_HTTP_CODE"
    exit 1
fi

# 6. Query in POST body only (no ?query= in URL).
echo "=== HTTP: Detach non-readonly with query in POST body only (no query= param) ==="
ASYNC_POST_BODY_ONLY=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}h3" -X POST --data-binary "INSERT INTO $TABLE SELECT 5" 2>/dev/null)
echo "Detach non-readonly POST body-only response (query_id): $([ -n "$ASYNC_POST_BODY_ONLY" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$ASYNC_POST_BODY_ONLY" ]; then
    echo "FAIL: Expected query_id when query is in POST body only"
    exit 1
fi

# Wait for detached INSERT (value 5) to complete.
wait_for_count 5
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
QUERY_ID_NATIVE=$($CLICKHOUSE_CLIENT --query_id "${QUERY_ID_PREFIX}native" --allow_experimental_detach_non_readonly_queries 1 --async_insert 0 -q "INSERT INTO $TABLE SELECT 7" 2>/dev/null || true)
echo "Detach response (query_id): $([ -n "$QUERY_ID_NATIVE" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$QUERY_ID_NATIVE" ]; then
    echo "FAIL: Expected non-empty query_id in response when allow_experimental_detach_non_readonly_queries=1"
    exit 1
fi

# Wait for detached INSERT (value 7) to complete.
wait_for_count 7

# 9. SELECT remains synchronous.
echo "=== Native: SELECT remains synchronous even with allow_experimental_detach_non_readonly_queries=1 ==="
SELECT_NATIVE=$($CLICKHOUSE_CLIENT --allow_experimental_detach_non_readonly_queries 1 -q "SELECT count() FROM $TABLE" 2>/dev/null)
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

# --- ExceptionBeforeStart: client receives the error, not a query_id ---
# These tests exercise the QueryStartedCallback path: on_query_started() is never called because
# executeQuery throws before logQueryStart, so started_promise->set_exception() is set and the
# client receives the error synchronously rather than a detached query_id.

# 11. HTTP: INSERT into nonexistent table — error is returned to client, not query_id.
echo "=== HTTP: ExceptionBeforeStart — error returned (not query_id) when query fails before start ==="
ERR_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&query_id=${QUERY_ID_PREFIX}noexist_http" \
    -X POST --data-binary "INSERT INTO table_that_does_not_exist_03812 SELECT 1" 2>/dev/null || true)
if echo "$ERR_HTTP" | grep -qi "UNKNOWN_TABLE\|Code:"; then
    echo "Error returned to client: yes"
else
    echo "Error returned to client: no (response: $ERR_HTTP)"
fi

# 12. Native: INSERT into nonexistent table — error is returned to client, not query_id.
echo "=== Native: ExceptionBeforeStart — error returned (not query_id) when query fails before start ==="
ERR_NATIVE=$(${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}noexist_native" --allow_experimental_detach_non_readonly_queries 1 \
    -q "INSERT INTO table_that_does_not_exist_03812 SELECT 1" 2>&1 || true)
if echo "$ERR_NATIVE" | grep -qi "UNKNOWN_TABLE\|doesn.*exist"; then
    echo "Error returned to client: yes"
else
    echo "Error returned to client: no (response: $ERR_NATIVE)"
fi

# --- system.processes: detached query is visible immediately after query_id is returned ---
# Because on_query_started() fires after ProcessList::insert (inside executeQueryImpl), the query
# is guaranteed to be in system.processes by the time the HTTP/native response is received.
# No sleep is required before the check — this determinism is the whole point of QueryStartedCallback.

# 13. HTTP: detached query appears in system.processes immediately.
echo "=== HTTP: Detached query visible in system.processes immediately after query_id returned ==="
QID_PROC_HTTP="${QUERY_ID_PREFIX}proc_http"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&async_insert=0&query_id=${QID_PROC_HTTP}" \
    -X POST --data-binary "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" > /dev/null
IN_PROC_HTTP=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${QID_PROC_HTTP}'" 2>/dev/null)
echo "Query visible in system.processes: $([ "${IN_PROC_HTTP}" -gt 0 ] && echo yes || echo no)"

# 14. Native: detached query appears in system.processes immediately.
echo "=== Native: Detached query visible in system.processes immediately after query_id returned ==="
QID_PROC_NATIVE="${QUERY_ID_PREFIX}proc_native"
${CLICKHOUSE_CLIENT} --query_id "${QID_PROC_NATIVE}" --allow_experimental_detach_non_readonly_queries 1 --async_insert 0 \
    -q "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" > /dev/null
IN_PROC_NATIVE=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${QID_PROC_NATIVE}'" 2>/dev/null)
echo "Query visible in system.processes: $([ "${IN_PROC_NATIVE}" -gt 0 ] && echo yes || echo no)"

sleep 6  # Wait for both sleep(3) queries above to finish before proceeding.

# --- ExceptionBeforeStart via duplicate query_id ---
# A second request using the same query_id as a still-running detached query hits
# QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING inside ProcessList::insert — still ExceptionBeforeStart.

# 15. HTTP: duplicate query_id while first is still running — error returned, not query_id.
echo "=== HTTP: ExceptionBeforeStart — duplicate query_id rejected ==="
QID_DUP_HTTP="${QUERY_ID_PREFIX}dup_http"
RESP_DUP1_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&async_insert=0&query_id=${QID_DUP_HTTP}" \
    -X POST --data-binary "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" 2>/dev/null)
echo "First detach response: $([ -n "$RESP_DUP1_HTTP" ] && echo '<query_id>' || echo '<error>')"
RESP_DUP2_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_non_readonly_queries=1&async_insert=0&query_id=${QID_DUP_HTTP}" \
    -X POST --data-binary "INSERT INTO ${TABLE} SELECT 1" 2>/dev/null || true)
if echo "$RESP_DUP2_HTTP" | grep -qi "QUERY_WITH_SAME_ID\|already running\|Code:"; then
    echo "Duplicate query_id error returned: yes"
else
    echo "Duplicate query_id error returned: no (response: $RESP_DUP2_HTTP)"
fi

# 16. Native: duplicate query_id while first is still running — error returned, not query_id.
echo "=== Native: ExceptionBeforeStart — duplicate query_id rejected ==="
QID_DUP_NATIVE="${QUERY_ID_PREFIX}dup_native"
RESP_DUP1_NATIVE=$(${CLICKHOUSE_CLIENT} --query_id "${QID_DUP_NATIVE}" --allow_experimental_detach_non_readonly_queries 1 --async_insert 0 \
    -q "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" 2>/dev/null || true)
echo "First detach response: $([ -n "$RESP_DUP1_NATIVE" ] && echo '<query_id>' || echo '<error>')"
RESP_DUP2_NATIVE=$(${CLICKHOUSE_CLIENT} --query_id "${QID_DUP_NATIVE}" --allow_experimental_detach_non_readonly_queries 1 --async_insert 0 \
    -q "INSERT INTO ${TABLE} SELECT 1" 2>&1 || true)
if echo "$RESP_DUP2_NATIVE" | grep -qi "QUERY_WITH_SAME_ID\|already running"; then
    echo "Duplicate query_id error returned: yes"
else
    echo "Duplicate query_id error returned: no (response: $RESP_DUP2_NATIVE)"
fi

sleep 6  # Wait for both sleep(3) dup queries above to finish.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE"
echo "OK"
