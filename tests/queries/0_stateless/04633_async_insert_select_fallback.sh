#!/usr/bin/env bash
# Tags: long
#   long: close to 3 minutes in flaky tests

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

urlencode() {
    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

# Case 1: multi-block SELECT (max_block_size=1) falls back to sync insert, no async-queue entry.
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

# Case 2: empty SELECT result stays synchronous, no async-queue entry.
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
# `insert_quorum_parallel=0` makes `InterpreterInsertQuery` route to the sync pipeline.
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

# Transactions are covered by 04633_async_insert_select_transaction.sql: they need an experimental
# server config, and an async insert inside one throws instead of falling back silently.

# Case 4: a trivial INSERT ... SELECT with more rows than max_block_size still routes through
# the async queue as one block, since `applyTrivialInsertSelectOptimization` raises the
# effective block size to `min_insert_block_size_rows` (~1M rows, well under 10 MiB here).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_trivial_opt"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_trivial_opt (n UInt64)
    ENGINE = MergeTree ORDER BY n
"
${CLICKHOUSE_CLIENT} --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_trivial_opt SELECT number AS n FROM numbers(200000)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_trivial_opt"

# The entry may not be visible after just one flush; retry a few times as a safety net.
for _ in $(seq 1 10); do
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
# Confirm the insert used the async queue, not the sync fallback.
${CLICKHOUSE_CLIENT} -q "
    SELECT count() >= 1
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_trivial_opt'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_trivial_opt"

# Case 5: a block with large per-row values must still respect `async_insert_max_data_size`.
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

# Case 6: `async_insert_max_data_size=1` exceeds the threshold, forcing a sync fallback.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_fallback"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_fallback (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
Q=$(urlencode "INSERT INTO test_async_sel_fallback SELECT number::UInt32 AS id, 'sync_' || toString(number) AS v FROM numbers(3)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_max_data_size=1&query=${Q}" -d ""
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_fallback ORDER BY id"
# Sync fallback must not appear in asynchronous_insert_log.
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

# Case 7: a Distributed destination always bypasses the async gate, even if the SELECT
# shape would otherwise qualify.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_dist"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_dist_local"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_dist_local (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_dist AS test_async_sel_dist_local
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), test_async_sel_dist_local, id)
"
Q=$(urlencode "INSERT INTO test_async_sel_dist SELECT number::UInt32 AS id, 'd_' || toString(number) AS v FROM numbers(3)")
${CLICKHOUSE_CURL} -sS -X POST \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&distributed_foreground_insert=1&query=${Q}" -d ""
# `test_cluster_two_shards` maps both shards onto the same node, so reading the Distributed
# table would double every row; read the local table instead.
# `distributed_foreground_insert=1` makes the INSERT wait for delivery to all shards.
${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM test_async_sel_dist_local ORDER BY id"
# Distributed destinations take the synchronous path, no asynchronous_insert_log entry.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_async_sel_dist'
"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_dist"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_dist_local"
