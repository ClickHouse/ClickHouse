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

# Case 6: timeout during pull must propagate instead of committing a prefix.
Q=$(urlencode "INSERT INTO test_async_input_fallback SELECT id, if(sleepEachRow(0.5) = 0, s, '') AS s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '6\ttimeout_row_a\n6\ttimeout_row_b\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&max_execution_time=0.3&query=${Q}&async_insert_max_data_size=1" \
    -H 'X-Test-Header: timeout_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 | grep -oF 'TIMEOUT_EXCEEDED'

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
    | grep 'X-ClickHouse-Progress' | tail -1 \
    | python3 -c "import sys,json; d=json.loads(sys.stdin.read().split(':',1)[1]); print(d['read_rows'])"

# Case 9: input('auto') with a missing destination throws UNKNOWN_TABLE.
Q=$(urlencode "INSERT INTO test_async_input_missing_auto SELECT * FROM input('auto') FORMAT TSV")
printf '9\tauto_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_experimental_analyzer=1&query=${Q}" \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 | grep -oF 'UNKNOWN_TABLE'

# Cleanup
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input_fallback"
