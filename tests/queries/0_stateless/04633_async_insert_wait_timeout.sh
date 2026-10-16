#!/usr/bin/env bash
# `wait_for_async_insert_timeout` ends the wait with TIMEOUT_EXCEEDED on both async insert routes
# (`INSERT ... SELECT` and `INSERT ... VALUES`), and the timed-out entry stays in the queue.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The busy timeout is far above the wait timeout, so only the explicit flush below drains the queue.
# Threads are pinned so the SELECT result stays one block and takes the queue route.
SETTINGS="async_insert = 1, async_insert_select_as_async_insert = 1,
    wait_for_async_insert = 1, wait_for_async_insert_timeout = 0.5,
    async_insert_use_adaptive_busy_timeout = 0,
    async_insert_busy_timeout_min_ms = 30000, async_insert_busy_timeout_max_ms = 30000,
    max_threads = 1, max_insert_threads = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_wait_timeout"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_wait_timeout (n UInt64) ENGINE = MergeTree ORDER BY n"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_async_wait_timeout SELECT number FROM numbers(3) SETTINGS $SETTINGS" 2>&1 \
    | grep -m1 -o "Wait for async insert timeout (500 ms) exceeded"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_async_wait_timeout SETTINGS $SETTINGS VALUES (10), (11)" 2>&1 \
    | grep -m1 -o "Wait for async insert timeout (500 ms) exceeded"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_async_wait_timeout"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_async_wait_timeout"
${CLICKHOUSE_CLIENT} -q "SELECT n FROM t_async_wait_timeout ORDER BY n"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_async_wait_timeout"
