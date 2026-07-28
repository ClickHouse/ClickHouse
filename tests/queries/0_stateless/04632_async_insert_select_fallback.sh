#!/usr/bin/env bash
# No tags needed: the non-parallel-quorum guard fires on the async_insert and
# insert_quorum_parallel=0 settings, independent of whether the destination is
# replicated. A plain MergeTree ignores the quorum setting, so the INSERT
# completes immediately and we can verify no async-queue entry was created.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

urlencode() {
    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

# Case 1: a multi-block result (max_block_size=1) is not a single block, so it falls back to a
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
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_multi"

# Case 2: an empty result inserts nothing and stays synchronous (no async-queue entry).
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
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_empty"

# Case 3: non-parallel quorum forces synchronous fallback (no async-queue entry).
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
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_quorum"

# Transaction fallback (implicit_transaction or getCurrentTransaction) is not covered by a
# stateless test here because MergeTree does not support transactions, so the insert would
# throw NOT_IMPLEMENTED before reaching the async eligibility check. The guard remains in
# the code for engines that do support transactions (ReplicatedMergeTree, SharedMergeTree).

# Case 4: trivial INSERT ... SELECT with N > max_block_size rows must route through the async
# queue as a single block, via applyTrivialInsertSelectOptimization raising max_block_size to
# min_insert_block_size_rows (~1M) so 200k rows arrive as one block (~1.6 MB < 10 MiB).
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

# Case 5: a block with a large per-row value must still respect async_insert_max_data_size.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_const_expand"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_const_expand (a UInt64, v String)
    ENGINE = MergeTree ORDER BY a
"
Q=$(urlencode "INSERT INTO test_async_sel_const_expand SELECT number AS a, repeat('x', 100000) AS v FROM numbers(100)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_max_data_size=1000000&query=${Q}" -d ""
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_const_expand"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_const_expand'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_const_expand"

# Case 6: when async_insert_max_data_size=1 the block exceeds the threshold and
# the implementation falls back to a synchronous INSERT ... SELECT. Rows still land.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_fallback"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_fallback (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
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
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_fallback"
