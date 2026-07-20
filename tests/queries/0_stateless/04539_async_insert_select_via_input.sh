#!/usr/bin/env bash

# Async HTTP INSERT ... SELECT FROM input() regression coverage.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

urlencode() {
    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_input_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_input_fallback"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_input (id UInt32, s String, hdr String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_input_tcp (id UInt32, s String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_input_fallback (id UInt32, s String, hdr String)
    ENGINE = MergeTree ORDER BY id
"

# Case 1: HTTP async path with input() and HTTP header capture.
Q=$(urlencode "INSERT INTO test_async_input SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '1\thello_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&query=${Q}" \
    -H 'X-Test-Header: hello' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

${CLICKHOUSE_CLIENT} -q "SELECT id, s, hdr FROM test_async_input ORDER BY id"

# asynchronous_insert_log may lag even after wait_for_async_insert=1.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND database = currentDatabase() AND table = 'test_async_input'")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done

# Preprocessed confirms pushQueryWithBlock.
${CLICKHOUSE_CLIENT} -q "
    SELECT status, data_kind
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_input'
    ORDER BY event_time_microseconds
    LIMIT 1
"

# Case 2: TCP path stays synchronous.
printf '2\tworld_row\n' | ${CLICKHOUSE_CLIENT} \
    --async_insert=1 \
    -q "INSERT INTO test_async_input_tcp SELECT id, s FROM input('id UInt32, s String') FORMAT TSV"

${CLICKHOUSE_CLIENT} -q "SELECT id, s FROM test_async_input_tcp ORDER BY id"

# TCP path never routes through the async queue.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_input_tcp'
"

# Case 3: table function destination keeps SELECT for structure hints.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_async_input"

Q=$(urlencode "INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'test_async_input') SELECT id, s, getClientHTTPHeader('X-Test-Header') AS hdr FROM input('id UInt32, s String') FORMAT TSV")
printf '3\tremote_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&allow_experimental_analyzer=1&query=${Q}" \
    -H 'X-Test-Header: remote_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

${CLICKHOUSE_CLIENT} -q "SELECT id, s, hdr FROM test_async_input ORDER BY id"

# Case 4: oversized payload falls back to sync without re-reading input().
Q=$(urlencode "INSERT INTO test_async_input_fallback SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '4\tfallback_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&async_insert_max_data_size=1&query=${Q}" \
    -H 'X-Test-Header: fallback_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

${CLICKHOUSE_CLIENT} -q "SELECT id, s, hdr FROM test_async_input_fallback ORDER BY id"

# This dedicated table should have no async log entry.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_input_fallback'
"

# Case 5: force_enable rejects non-stable SELECT before input() is consumed.
Q=$(urlencode "INSERT INTO test_async_input SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '5\tdedup_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&deduplicate_insert_select=force_enable&query=${Q}" \
    -H 'X-Test-Header: dedup_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 | grep -oF 'DEDUPLICATION_IS_NOT_POSSIBLE'

# Case 6: a timeout during the pull must fail the query, not report success.
# It surfaces as TIMEOUT_EXCEEDED or, occasionally, QUERY_WAS_CANCELLED; accept either.
# (Like a synchronous INSERT ... SELECT, an aborted fallback may still leave a committed prefix.)
Q=$(urlencode "INSERT INTO test_async_input_fallback SELECT id, if(sleepEachRow(0.5) = 0, s, '') AS s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '6\ttimeout_row_a\n6\ttimeout_row_b\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&max_execution_time=0.3&query=${Q}&async_insert_max_data_size=1" \
    -H 'X-Test-Header: timeout_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 \
    | grep -oE 'TIMEOUT_EXCEEDED|QUERY_WAS_CANCELLED' | head -n1 \
    | sed 's/QUERY_WAS_CANCELLED/TIMEOUT_EXCEEDED/'

# Case 7: missing destination must fail before input() is pulled.
# The long sleep only runs on broken binaries, so max-time can be generous.
Q=$(urlencode "INSERT INTO test_async_input_missing SELECT id, if(sleepEachRow(2) = 0, s, '') AS s, '' FROM input('id UInt32, s String') FORMAT TSV")
seq 1 20 | sed 's/$/\tx/' | ${CLICKHOUSE_CURL} -sS --max-time 10 \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&query=${Q}" \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 | grep -oF 'UNKNOWN_TABLE'

# Case 8: waiting for async flush must not double-count SELECT read progress.
Q=$(urlencode "INSERT INTO test_async_input SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '8\tprogress_a\n8\tprogress_b\n8\tprogress_c\n' | ${CLICKHOUSE_CURL} -sS -D - \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&query=${Q}" \
    -H 'X-Test-Header: progress_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- \
    | grep 'X-ClickHouse-Progress' \
    | python3 -c "import sys, json; rows = [json.loads(l.split(':', maxsplit=1)[1])['read_rows'] for l in sys.stdin if ':' in l]; print(max((int(r) for r in rows), default=''))"

# Case 9: input('auto') with a missing destination throws UNKNOWN_TABLE.
Q=$(urlencode "INSERT INTO test_async_input_missing_auto SELECT * FROM input('auto') FORMAT TSV")
printf '9\tauto_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_experimental_analyzer=1&query=${Q}" \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 | grep -oF 'UNKNOWN_TABLE'

# Case 10: KILL QUERY interrupts the async SELECT pull before the block is committed.
# Proves the inner SELECT pipeline is registered with the process list via
# BuildQueryPipelineSettings, not only caught by the post-loop check.
QID="04539_kill_${CLICKHOUSE_DATABASE}_$$"
Q=$(urlencode "INSERT INTO test_async_input SELECT id, if(sleepEachRow(2) = 0, s, '') AS s, '' FROM input('id UInt32, s String') FORMAT TSV")
seq 1 3 | sed 's/$/\tkill_row/' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&function_sleep_max_microseconds_per_block=15000000&query_id=${QID}&query=${Q}" \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- > /dev/null 2>&1 &
CURL_PID=$!
for _ in $(seq 1 30); do
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${QID}'" | grep -q '^1$' && break
    sleep 0.3
done
${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${QID}' SYNC" > /dev/null
wait $CURL_PID
for _ in $(seq 1 30); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${QID}' AND exception LIKE '%QUERY_WAS_CANCELLED%'")
    [ "$count" -ge 1 ] && break
    sleep 0.3
done
${CLICKHOUSE_CLIENT} -q "SELECT 'QUERY_WAS_CANCELLED' FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${QID}' AND exception LIKE '%QUERY_WAS_CANCELLED%' LIMIT 1"

# Cleanup
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input_fallback"
