#!/usr/bin/env bash
# Exercises basic async insert routing across protocols; no tags needed.
# async_insert defaults on for this server; the setting below is made explicit only for clarity.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

urlencode() {
    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_wait"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_input (id UInt32, v String, hdr String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_tcp (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_wait (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"

# Case 1: plain INSERT ... SELECT over HTTP routes to the async queue.
# wait_for_async_insert is forced to 1, so rows are visible right after the HTTP response returns.
Q=$(urlencode "INSERT INTO test_async_sel SELECT number::UInt32 AS id, 'async_' || toString(number) AS v FROM numbers(3)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query=${Q}" -d ""
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel ORDER BY id"

# One flush after the insert should already see it; short retry is just a safety net.
for _ in $(seq 1 10); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    count=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND database = currentDatabase()
          AND table = 'test_async_sel'
    ")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done

# Confirms a Preprocessed entry exists, i.e. pushQueryWithBlock was used.
${CLICKHOUSE_CLIENT} -q "
    SELECT status, data_kind
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel'
    ORDER BY event_time_microseconds
    LIMIT 1
"

# Case 2: INSERT ... SELECT FROM input() over HTTP with getClientHTTPHeader still works,
# i.e. general SELECT routing does not break the input() path.
Q=$(urlencode "INSERT INTO test_async_sel_input SELECT id, v, getClientHTTPHeader('X-My-Header') FROM input('id UInt32, v String') FORMAT TSV")
printf '10\tinput_val\n' | ${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&query=${Q}" \
    -H 'X-My-Header: captured_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-
${CLICKHOUSE_CLIENT} -q "SELECT id, v, hdr FROM test_async_sel_input ORDER BY id"

# Case 3: same routing applies over native TCP (clickhouse-client), not only HTTP.
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_tcp SELECT number::UInt32 AS id, 'tcp_' || toString(number) AS v FROM numbers(2)
"
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_tcp ORDER BY id"

for _ in $(seq 1 10); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    count=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND database = currentDatabase()
          AND table = 'test_async_sel_tcp'
    ")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done
${CLICKHOUSE_CLIENT} -q "
    SELECT count() >= 1
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_tcp'
"

# Case 4: with wait_for_async_insert=0 the call returns once the block is queued, so visibility
# is not guaranteed yet; poll until the rows land before selecting.
Q=$(urlencode "INSERT INTO test_async_sel_wait SELECT number::UInt32 AS id, 'wait_' || toString(number) AS v FROM numbers(2)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=${Q}" -d ""

for _ in $(seq 1 20); do
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_wait")
    [ "$count" -ge 2 ] && break
    sleep 0.5
done
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_wait ORDER BY id"

# Cleanup.
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_wait"
