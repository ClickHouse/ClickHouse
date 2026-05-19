#!/usr/bin/env bash
# Test `allow_experimental_detach_queries` for HTTP and native protocol: when enabled, any
# detachable query (INSERT-SELECT, SELECT, ALTER, ...) is dispatched to a background thread and
# the server returns immediately with a result that contains its `query_id`.
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
# 1. With allow_experimental_detach_queries=0 (default), INSERT-SELECT runs synchronously.
echo "=== HTTP: Sync mode (default): INSERT-SELECT waits for completion ==="
BODY_SYNC=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+$TABLE+SELECT+1" -X POST -d "" 2>/dev/null || true)
HEADER_SYNC=$(${CLICKHOUSE_CURL} -sS -D - "${CLICKHOUSE_URL}&query=INSERT+INTO+$TABLE+SELECT+2" -X POST -d "" -o /dev/null 2>/dev/null | grep -i "X-ClickHouse-Query-Id" || true)
echo "Sync response body length: ${#BODY_SYNC}"
echo 'Sync has X-ClickHouse-Query-Id header: '"$( [ -n "$HEADER_SYNC" ] && echo yes || echo no )"

# 2. With allow_experimental_detach_queries=1, INSERT-SELECT returns immediately with query_id in body and header.
echo "=== HTTP: Detach mode: INSERT-SELECT returns immediately with query_id ==="
QUERY_ID_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}h1&query=INSERT+INTO+$TABLE+SELECT+3" -X POST -d "" 2>/dev/null || true)
echo "Detach response body (query_id): $([ -n "$QUERY_ID_HTTP" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$QUERY_ID_HTTP" ]; then
    echo "FAIL: Expected non-empty query_id in response body when allow_experimental_detach_queries=1"
    exit 1
fi

# Wait for detached INSERT (value 3) to complete (slow CI may need more than a fixed sleep).
wait_for_count 3

# 3. SELECT is also detachable now (returns query_id, not the rows).
echo "=== HTTP: SELECT is detached when allow_experimental_detach_queries=1 ==="
SELECT_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}h_select&query=SELECT+count()+FROM+$TABLE" -X POST -d "" 2>/dev/null)
echo "Detached SELECT response (query_id): $([ -n "$SELECT_HTTP" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$SELECT_HTTP" ]; then
    echo "FAIL: SELECT should return query_id when detach is enabled"
    exit 1
fi

# 3b. SELECT runs synchronously when the setting is off.
SELECT_HTTP_SYNC=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+count()+FROM+$TABLE" -X POST -d "" 2>/dev/null)
echo "Sync SELECT result: $SELECT_HTTP_SYNC"

# 4. Verify data (1, 2 sync + 3 detach = 3 rows).
COUNT_HTTP=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE" 2>/dev/null)
echo "Rows in table after HTTP detached inserts: $COUNT_HTTP"
if [ "$COUNT_HTTP" -lt 3 ]; then
    echo "FAIL: Expected at least 3 rows (sync 1,2 and detach 3), got $COUNT_HTTP"
    exit 1
fi

# 5. INSERT with body data (query in URL, data in body) is also detached — the body is captured and passed to the background thread.
echo "=== HTTP: INSERT with body data is detached (query in URL, data in POST body) ==="
INSERT_BODY_RESP=$(${CLICKHOUSE_CURL} -sS -w "\n%{http_code}" "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}h2&query=INSERT+INTO+$TABLE+FORMAT+TabSeparated" -X POST -d "4" 2>/dev/null || echo "000")
INSERT_BODY_HTTP_CODE=$(echo "$INSERT_BODY_RESP" | tail -n1)
INSERT_BODY_BODY=$(echo "$INSERT_BODY_RESP" | sed '$d')
echo "INSERT with body HTTP code: $INSERT_BODY_HTTP_CODE"
echo "INSERT with body response has query_id: $([ -n "$INSERT_BODY_BODY" ] && echo yes || echo no)"
if [ "$INSERT_BODY_HTTP_CODE" != "200" ]; then
    echo "FAIL: Expected HTTP 200 for detached INSERT with body, got $INSERT_BODY_HTTP_CODE"
    exit 1
fi
if [ -z "$INSERT_BODY_BODY" ]; then
    echo "FAIL: Expected query_id in response body for detached INSERT with body data, got empty"
    exit 1
fi

# 6. Query in POST body only (no ?query= in URL).
echo "=== HTTP: Detach with query in POST body only (no query= param) ==="
ASYNC_POST_BODY_ONLY=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}h3" -X POST --data-binary "INSERT INTO $TABLE SELECT 5" 2>/dev/null)
echo "Detach POST body-only response (query_id): $([ -n "$ASYNC_POST_BODY_ONLY" ] && echo '<query_id>' || echo '<empty>')"
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
echo "=== Native: Detach mode: INSERT...SELECT returns immediately with query_id ==="
QUERY_ID_NATIVE=$($CLICKHOUSE_CLIENT --query_id "${QUERY_ID_PREFIX}native" --allow_experimental_detach_queries 1 --async_insert 0 -q "INSERT INTO $TABLE SELECT 7" 2>/dev/null || true)
echo "Detach response (query_id): $([ -n "$QUERY_ID_NATIVE" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$QUERY_ID_NATIVE" ]; then
    echo "FAIL: Expected non-empty query_id in response when allow_experimental_detach_queries=1"
    exit 1
fi

# Wait for detached INSERT (value 7) to complete.
wait_for_count 7

# 9. Native: SELECT is also detachable (returns query_id, not rows).
echo "=== Native: SELECT is detached when allow_experimental_detach_queries=1 ==="
SELECT_NATIVE=$($CLICKHOUSE_CLIENT --query_id "${QUERY_ID_PREFIX}n_select" --allow_experimental_detach_queries 1 --async_insert 0 -q "SELECT count() FROM $TABLE" 2>/dev/null)
echo "Detached SELECT response (query_id): $([ -n "$SELECT_NATIVE" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$SELECT_NATIVE" ]; then
    echo "FAIL: SELECT should return query_id when detach is enabled"
    exit 1
fi

# 9b. Sync SELECT for the count check.
SELECT_NATIVE_SYNC=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $TABLE" 2>/dev/null)
echo "Sync SELECT result: $SELECT_NATIVE_SYNC"

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
ERR_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QUERY_ID_PREFIX}noexist_http" \
    -X POST --data-binary "INSERT INTO table_that_does_not_exist_03812 SELECT 1" 2>/dev/null || true)
if echo "$ERR_HTTP" | grep -qi "UNKNOWN_TABLE\|Code:"; then
    echo "Error returned to client: yes"
else
    echo "Error returned to client: no (response: $ERR_HTTP)"
fi

# 12. Native: INSERT into nonexistent table — error is returned to client, not query_id.
echo "=== Native: ExceptionBeforeStart — error returned (not query_id) when query fails before start ==="
ERR_NATIVE=$(${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID_PREFIX}noexist_native" --allow_experimental_detach_queries 1 --async_insert 0 \
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
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QID_PROC_HTTP}" \
    -X POST --data-binary "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" > /dev/null
IN_PROC_HTTP=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${QID_PROC_HTTP}'" 2>/dev/null)
echo "Query visible in system.processes: $([ "${IN_PROC_HTTP}" -gt 0 ] && echo yes || echo no )"

# 14. Native: detached query appears in system.processes immediately.
echo "=== Native: Detached query visible in system.processes immediately after query_id returned ==="
QID_PROC_NATIVE="${QUERY_ID_PREFIX}proc_native"
${CLICKHOUSE_CLIENT} --query_id "${QID_PROC_NATIVE}" --allow_experimental_detach_queries 1 --async_insert 0 \
    -q "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" > /dev/null
IN_PROC_NATIVE=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${QID_PROC_NATIVE}'" 2>/dev/null)
echo "Query visible in system.processes: $([ "${IN_PROC_NATIVE}" -gt 0 ] && echo yes || echo no )"

# 14b. HTTP: detached SELECT is also visible in system.processes (proves it uses the same path).
echo "=== HTTP: Detached SELECT visible in system.processes ==="
QID_SELECT_HTTP="${QUERY_ID_PREFIX}select_proc_http"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QID_SELECT_HTTP}" \
    -X POST --data-binary "SELECT sleep(3), count() FROM ${TABLE}" > /dev/null
IN_PROC_SELECT=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${QID_SELECT_HTTP}'" 2>/dev/null)
echo "Detached SELECT visible in system.processes: $([ "${IN_PROC_SELECT}" -gt 0 ] && echo yes || echo no )"

sleep 6  # Wait for the three sleep(3) queries above to finish before proceeding.

# --- ExceptionBeforeStart via duplicate query_id ---
# A second request using the same query_id as a still-running detached query hits
# QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING inside ProcessList::insert — still ExceptionBeforeStart.

# 15. HTTP: duplicate query_id while first is still running — error returned, not query_id.
echo "=== HTTP: ExceptionBeforeStart — duplicate query_id rejected ==="
QID_DUP_HTTP="${QUERY_ID_PREFIX}dup_http"
RESP_DUP1_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QID_DUP_HTTP}" \
    -X POST --data-binary "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" 2>/dev/null)
echo "First detach response: $([ -n "$RESP_DUP1_HTTP" ] && echo '<query_id>' || echo '<error>')"
RESP_DUP2_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&async_insert=0&query_id=${QID_DUP_HTTP}" \
    -X POST --data-binary "INSERT INTO ${TABLE} SELECT 1" 2>/dev/null || true)
if echo "$RESP_DUP2_HTTP" | grep -qi "QUERY_WITH_SAME_ID\|already running\|Code:"; then
    echo "Duplicate query_id error returned: yes"
else
    echo "Duplicate query_id error returned: no (response: $RESP_DUP2_HTTP)"
fi

# 16. Native: duplicate query_id while first is still running — error returned, not query_id.
echo "=== Native: ExceptionBeforeStart — duplicate query_id rejected ==="
QID_DUP_NATIVE="${QUERY_ID_PREFIX}dup_native"
RESP_DUP1_NATIVE=$(${CLICKHOUSE_CLIENT} --query_id "${QID_DUP_NATIVE}" --allow_experimental_detach_queries 1 --async_insert 0 \
    -q "INSERT INTO ${TABLE} SELECT toUInt64(sleep(3))" 2>/dev/null || true)
echo "First detach response: $([ -n "$RESP_DUP1_NATIVE" ] && echo '<query_id>' || echo '<error>')"
RESP_DUP2_NATIVE=$(${CLICKHOUSE_CLIENT} --query_id "${QID_DUP_NATIVE}" --allow_experimental_detach_queries 1 --async_insert 0 \
    -q "INSERT INTO ${TABLE} SELECT 1" 2>&1 || true)
if echo "$RESP_DUP2_NATIVE" | grep -qi "QUERY_WITH_SAME_ID\|already running"; then
    echo "Duplicate query_id error returned: yes"
else
    echo "Duplicate query_id error returned: no (response: $RESP_DUP2_NATIVE)"
fi

sleep 6  # Wait for both sleep(3) dup queries above to finish.

# --- Inline SETTINGS in the query text trigger detach ---
# The setting can be passed as SETTINGS clause inside the SQL itself, without URL params or client flags.

# 17. HTTP: detach triggered by inline SETTINGS in POST body.
echo "=== HTTP: Detach triggered by inline SETTINGS in query body ==="
QID_INLINE_HTTP="${QUERY_ID_PREFIX}inline_http"
RESP_INLINE_HTTP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0&query_id=${QID_INLINE_HTTP}" \
    -X POST --data-binary "INSERT INTO ${TABLE} SELECT 8 SETTINGS allow_experimental_detach_queries=1" 2>/dev/null || true)
echo "Inline SETTINGS detach response: $([ -n "$RESP_INLINE_HTTP" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$RESP_INLINE_HTTP" ]; then
    echo "FAIL: Expected query_id when allow_experimental_detach_queries=1 is in inline SETTINGS"
    exit 1
fi
wait_for_count 8

# 18. Native: detach triggered by inline SETTINGS in query text.
echo "=== Native: Detach triggered by inline SETTINGS in query text ==="
QID_INLINE_NATIVE="${QUERY_ID_PREFIX}inline_native"
RESP_INLINE_NATIVE=$($CLICKHOUSE_CLIENT --query_id "${QID_INLINE_NATIVE}" \
    -q "INSERT INTO ${TABLE} SELECT 9 SETTINGS allow_experimental_detach_queries=1, async_insert=0" 2>/dev/null || true)
echo "Inline SETTINGS detach response: $([ -n "$RESP_INLINE_NATIVE" ] && echo '<query_id>' || echo '<empty>')"
if [ -z "$RESP_INLINE_NATIVE" ]; then
    echo "FAIL: Expected query_id when allow_experimental_detach_queries=1 is in inline SETTINGS"
    exit 1
fi
wait_for_count 9

# --- Session-mutating queries are not detachable: SET/USE/etc. run synchronously even when the setting is on ---
# `IAST::isDetachableQuery` returns false for SET/USE/BEGIN/COMMIT/ROLLBACK/KILL QUERY, so they
# fall through to the sync path and return their normal result instead of a query_id.

# 19. SET runs synchronously (returns empty body, not a query_id) so the next query in the same session sees max_threads=1.
echo "=== HTTP: SET runs synchronously even with allow_experimental_detach_queries=1 ==="
SET_RESP=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_detach_queries=1&query=SET+max_threads+%3D+1" -X POST -d "" 2>/dev/null)
echo "SET response body length: ${#SET_RESP}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE"
echo "OK"
