#!/usr/bin/env bash
# Tags: no-fasttest

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
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_input (id UInt32, s String, hdr String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_input_tcp (id UInt32, s String)
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
# the limit. The fallback must write synchronously without re-executing the
# already-consumed StorageInput pipeline (was_pipe_used guard).
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_async_input"

Q=$(urlencode "INSERT INTO test_async_input SELECT id, s, getClientHTTPHeader('X-Test-Header') FROM input('id UInt32, s String') FORMAT TSV")
printf '4\tfallback_row\n' | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&async_insert_max_data_size=1&query=${Q}" \
    -H 'X-Test-Header: fallback_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

# Data must have arrived synchronously.
${CLICKHOUSE_CLIENT} -q "SELECT id, s, hdr FROM test_async_input ORDER BY id"

# The oversized fallback writes synchronously — no async log entry for this insert.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 30
      AND database = currentDatabase()
      AND table = 'test_async_input'
"

# ── Cleanup ──────────────────────────────────────────────────────────────────
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_input_tcp"
