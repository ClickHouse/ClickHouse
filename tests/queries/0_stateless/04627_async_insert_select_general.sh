#!/usr/bin/env bash
# No tags needed: the non-parallel-quorum guard fires on the async_insert and
# insert_quorum_parallel=0 settings, independent of whether the destination is
# replicated. A plain MergeTree ignores the quorum setting, so the INSERT
# completes immediately and we can verify no async-queue entry was created.

# Note: async_insert defaults on for this server, so the setting is only made explicit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

urlencode() {
    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_fallback"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_wait"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_fallback (id UInt32, v String)
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
# wait_for_async_insert is forced to 1 by the implementation so rows are
# visible immediately after the HTTP response is received.
Q=$(urlencode "INSERT INTO test_async_sel SELECT number::UInt32 AS id, 'async_' || toString(number) AS v FROM numbers(3)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query=${Q}" -d ""

# Case 4: rows must be present immediately after the HTTP call returns.
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel ORDER BY id"

# asynchronous_insert_log can lag even after wait_for_async_insert=1; poll with bounded retry.
for _ in $(seq 1 60); do
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

# Confirm a Preprocessed entry exists confirming pushQueryWithBlock was used.
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

# Case 2: when async_insert_max_data_size=1 the block exceeds the threshold and
# the implementation falls back to a synchronous INSERT ... SELECT. Rows still land.
Q=$(urlencode "INSERT INTO test_async_sel_fallback SELECT number::UInt32 AS id, 'sync_' || toString(number) AS v FROM numbers(3)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_max_data_size=1&query=${Q}" -d ""

${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_fallback ORDER BY id"

# The fallback table must NOT appear in asynchronous_insert_log; sync path bypasses the queue.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_fallback'
"

# Case 3: INSERT ... SELECT FROM input() over HTTP with getClientHTTPHeader still works.
# This validates that the input() path is not broken by the general SELECT routing.
Q=$(urlencode "INSERT INTO test_async_sel_input SELECT id, v, getClientHTTPHeader('X-My-Header') FROM input('id UInt32, v String') FORMAT TSV")
printf '10\tinput_val\n' | ${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&allow_get_client_http_header=1&query=${Q}" \
    -H 'X-My-Header: captured_hdr' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @-

# Case 4 for input(): data visible immediately after HTTP response.
${CLICKHOUSE_CLIENT} -q "SELECT id, v, hdr FROM test_async_sel_input ORDER BY id"

# Case 5: the same routing applies over the native TCP protocol (clickhouse-client),
# not only HTTP. A single small block still goes through the async queue.
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_tcp SELECT number::UInt32 AS id, 'tcp_' || toString(number) AS v FROM numbers(2)
"
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_tcp ORDER BY id"

for _ in $(seq 1 60); do
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

# Case 6: wait_for_async_insert is forced on this path. Even with wait_for_async_insert=0
# the rows are visible immediately after the HTTP call returns.
Q=$(urlencode "INSERT INTO test_async_sel_wait SELECT number::UInt32 AS id, 'wait_' || toString(number) AS v FROM numbers(2)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=${Q}" -d ""
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_wait ORDER BY id"

# Case 7: a multi-block result (max_block_size=1) is not a single block, so it falls back to a
# synchronous insert. All rows land and there is no async-queue entry for this table.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_multi"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_multi (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
Q=$(urlencode "INSERT INTO test_async_sel_multi SELECT number::UInt32 AS id, 'm_' || toString(number) AS v FROM numbers(5) SETTINGS max_block_size = 1")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query=${Q}" -d ""
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_multi"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_multi'
"

# Case 8: an empty result inserts nothing and stays synchronous (no async-queue entry).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_empty"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_empty (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
Q=$(urlencode "INSERT INTO test_async_sel_empty SELECT number::UInt32 AS id, '' AS v FROM numbers(0)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query=${Q}" -d ""
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_empty"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_empty'
"

# Case 9: non-parallel quorum forces synchronous fallback (no async-queue entry).
# insert_quorum=auto resolves to 1 (majority of 1 replica), which is satisfied
# immediately; insert_quorum_parallel=0 makes the insert non-parallel so the guard
# in InterpreterInsertQuery routes it to the synchronous pipeline.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_quorum"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_quorum (id UInt32, v String)
    ENGINE = MergeTree
    ORDER BY id
"
Q=$(urlencode "INSERT INTO test_async_sel_quorum SELECT number::UInt32 AS id, 'q_' || toString(number) AS v FROM numbers(3)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&insert_quorum=auto&insert_quorum_parallel=0&query=${Q}" -d ""
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_quorum ORDER BY id"
# Non-parallel quorum uses the synchronous pipeline; the async queue must have no entry.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_quorum'
"

# Transaction fallback (implicit_transaction or getCurrentTransaction) is not covered by a
# stateless test here because MergeTree does not support transactions, so the insert would
# throw NOT_IMPLEMENTED before reaching the async eligibility check. The guard remains in
# the code for engines that do support transactions (ReplicatedMergeTree, SharedMergeTree).

# Case 10 (regression Fix A): an empty INSERT ... SELECT must still execute the insert pipeline
# so that side-effecting destinations (file table functions, etc.) are created even when SELECT
# returns zero rows. Mirror the 03277 pattern: write to a CSV file from an empty Join table,
# then read back from the file; if the file was never created this FROM INFILE fails.
FILE_EMPTY="${CLICKHOUSE_USER_FILES_UNIQUE:?}_04627_empty.csv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_empty_src"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_empty_src (id UInt32)
    ENGINE = Join(ANY, INNER, id)
"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO TABLE FUNCTION file('${FILE_EMPTY}', 'CSV', 'id UInt32')
    SELECT id FROM test_async_sel_empty_src
"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_async_sel_empty_src (id)
    FROM INFILE '${FILE_EMPTY}' FORMAT CSV
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_empty_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_empty_src"
rm -f "${FILE_EMPTY}"

# Case 11 (regression Fix B): multi-block INSERT ... SELECT with a Nullable/expression column
# into a table with a Nullable column must not crash with a schema-conversion logical error.
# max_block_size=1 forces the multi-block fallback path.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_nullable"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_nullable (v Nullable(UInt64))
    ENGINE = MergeTree ORDER BY tuple()
"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_nullable
    SELECT toNullable(number) AS v FROM numbers(5)
    SETTINGS max_block_size = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_nullable"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_nullable"

# Case 12: trivial INSERT ... SELECT with N > max_block_size rows must route through the async
# queue as a single block. Without the fix, the SELECT uses the default max_block_size (~65k)
# and 200k rows produce two blocks, causing a spurious sync fallback. With
# applyTrivialInsertSelectOptimization applied in the async path, max_block_size is raised to
# min_insert_block_size_rows (~1M), so all 200k rows arrive as one block (~1.6 MB < 10 MiB).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_trivial_opt"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_trivial_opt (n UInt64)
    ENGINE = MergeTree ORDER BY n
"
${CLICKHOUSE_CLIENT} --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_trivial_opt SELECT number AS n FROM numbers(200000)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_trivial_opt"

for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    count=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND database = currentDatabase()
          AND table = 'test_async_sel_trivial_opt'
    ")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done
# Confirm the insert went through the async queue (not the sync fallback).
${CLICKHOUSE_CLIENT} -q "
    SELECT count() >= 1
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_trivial_opt'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_trivial_opt"

# Case 13: concurrent MODIFY COLUMN FIRST must not corrupt data (MatchColumnsMode::Name regression).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_alter_race"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_alter_race (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=500000 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_alter_race
    SELECT number AS a, number * 2 AS b
    FROM numbers(1000000)
    WHERE sleepEachRow(0.000002) = 0
" &
INSERT_PID=$!
sleep 0.5
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_alter_race MODIFY COLUMN b UInt32 FIRST"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_alter_race WHERE b != a * 2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_alter_race"

# Case 14: concurrent ADD COLUMN must not cause THERE_IS_NO_COLUMN (schema freeze, async path).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_add_col_single"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_add_col_single (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_add_col_single
    SELECT number AS a, number * 2 AS b
    FROM numbers(200000)
    WHERE sleepEachRow(0.000002) = 0
" &
INSERT_PID=$!
sleep 0.2
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_add_col_single ADD COLUMN c UInt32 DEFAULT 42"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single WHERE b = a * 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single WHERE c = 42"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_add_col_single"

# Case 15: same ADD COLUMN race on the sync-fallback path (schema freeze, multi-block).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_add_col_multi"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_add_col_multi (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=500000 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_add_col_multi
    SELECT number AS a, number * 2 AS b
    FROM numbers(1000000)
    WHERE sleepEachRow(0.000002) = 0
" &
INSERT_PID=$!
sleep 0.5
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_add_col_multi ADD COLUMN c UInt32 DEFAULT 42"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi WHERE b = a * 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi WHERE c = 42"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_add_col_multi"

# Cleanup.
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_fallback"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_input"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_tcp"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_wait"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_multi"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_empty"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_quorum"
