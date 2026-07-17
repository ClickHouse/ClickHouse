#!/usr/bin/env bash

# Test that INSERT...SELECT FROM input() is routed through the async insert
# queue automatically when async_insert=1, verified via system.asynchronous_insert_log.

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
# Dedicated table for Case 4 so its "no async entry" check is not polluted by
# the async inserts into test_async_input from the earlier cases.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_input_fallback (id UInt32, s String, hdr String)
    ENGINE = MergeTree ORDER BY id
"

# ── Case 1: HTTP + async_insert=1 — INSERT...SELECT FROM input() via async queue ─
# Send data via HTTP with a custom header; the query reads it through input()
# and stores the header value via getClientHTTPHeader().
Q=$(urlencode "INSERT INTO test_async_input SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '1\thello_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&query=${Q}" \
    -H 'X-Test-Header: hello' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

# Verify data arrived correctly and the header value was preserved.
${CLICKHOUSE_CLIENT} -q "SELECT id, s, hdr FROM test_async_input ORDER BY id"

# asynchronous_insert_log may not appear immediately even after wait_for_async_insert=1
# (see https://github.com/ClickHouse/ClickHouse/issues/84364).
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND database = currentDatabase() AND table = 'test_async_input'")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done

# status=Ok and data_kind=Preprocessed confirm the block was pushed via pushQueryWithBlock.
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

# ── Case 2: TCP + async_insert=1 — INSERT...SELECT FROM input() stays sync ───────
# Over TCP, tail is always null, so async_insert_select_via_input is never set.
# The query must execute synchronously regardless of async_insert=1.
printf '2\tworld_row\n' | ${CLICKHOUSE_CLIENT} \
    --async_insert=1 \
    -q "INSERT INTO test_async_input_tcp SELECT id, s FROM input('id UInt32, s String') FORMAT TSV"

# Verify data arrived (sync execution completes before the client returns).
${CLICKHOUSE_CLIENT} -q "SELECT id, s FROM test_async_input_tcp ORDER BY id"

# Confirm no async log entries — TCP path never routes via the async queue.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_input_tcp'
"

# ── Case 3: table function destination (remote) + async_insert=1 ─────────────────
# Regression: async_insert_flush must be set on the queued AST so that
# InterpreterInsertQuery::getTable can still call setStructureHint for table
# functions, while execute() does not re-run the already-consumed SELECT.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_async_input"

Q=$(urlencode "INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'test_async_input') SELECT id, s, getClientHTTPHeader('X-Test-Header') AS hdr FROM input('id UInt32, s String') FORMAT TSV")
printf '3\tremote_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&allow_experimental_analyzer=1&query=${Q}" \
    -H 'X-Test-Header: remote_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

${CLICKHOUSE_CLIENT} -q "SELECT id, s, hdr FROM test_async_input ORDER BY id"

# ── Case 4: oversized payload → sync fallback ─────────────────────────────────────
# Regression: with async_insert_max_data_size=1, every non-empty block exceeds
# the limit, so the payload is written synchronously. The fallback must not
# re-read the already-consumed input() body (the clone drops its inlined-data
# markers so buildInsertPipeline stays a pushing pipeline).
Q=$(urlencode "INSERT INTO test_async_input_fallback SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '4\tfallback_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&async_insert_max_data_size=1&query=${Q}" \
    -H 'X-Test-Header: fallback_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

# Data must have arrived synchronously.
${CLICKHOUSE_CLIENT} -q "SELECT id, s, hdr FROM test_async_input_fallback ORDER BY id"

# The oversized fallback writes synchronously — no async log entry for this
# dedicated table (isolated from the async inserts of the earlier cases).
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_input_fallback'
"

# ── Case 5: deduplicate_insert_select=force_enable rejects non-stable SELECT ─────
# With force_enable and no ORDER BY ALL / insert_deduplication_token, the async
# path must throw DEDUPLICATION_IS_NOT_POSSIBLE *before* consuming input(), just
# as the synchronous INSERT...SELECT path does.
Q=$(urlencode "INSERT INTO test_async_input SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '5\tdedup_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&deduplicate_insert_select=force_enable&query=${Q}" \
    -H 'X-Test-Header: dedup_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 | grep -oF 'DEDUPLICATION_IS_NOT_POSSIBLE'

# ── Case 6: timeout during SELECT pull → error, no silent partial commit ─────────
# sleepEachRow(0.5) on two rows = ~1 s; max_execution_time=0.3 s fires during
# the pull loop. The async path must propagate TIMEOUT_EXCEEDED rather than
# quietly queuing or committing the prefix it already pulled.
Q=$(urlencode "INSERT INTO test_async_input_fallback SELECT id, if(sleepEachRow(0.5) = 0, s, '') AS s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '6\ttimeout_row_a\n6\ttimeout_row_b\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&max_execution_time=0.3&query=${Q}&async_insert_max_data_size=1" \
    -H 'X-Test-Header: timeout_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- 2>&1 | grep -oF 'TIMEOUT_EXCEEDED'

# ── Cleanup ──────────────────────────────────────────────────────────────────
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input_fallback"
