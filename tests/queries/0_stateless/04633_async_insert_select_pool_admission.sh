#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint pauses the scheduling of every async insert flush on the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pushing a block to the queue can wait for a free flush worker. An async `INSERT ... SELECT` must not
# hold the destination lock meanwhile: the flush that would free the worker may wait behind a writer
# queued on that lock. The failpoint stands for the busy pool; `POPULATE` takes the write lock on its
# source, and its short timeout bounds what holding the lock costs.

function cleanup() {
	${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT async_insert_pause_before_schedule"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst (n UInt64) ENGINE = MergeTree ORDER BY n"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT async_insert_pause_before_schedule"

# The 3-row `UInt64` block is exactly `async_insert_max_data_size` bytes: still small enough for the queue,
# and big enough for the push to schedule the flush itself. A query count would do so only with deduplication.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO dst SELECT number FROM numbers(3)
    SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_max_data_size = 24,
             async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_max_ms = 600000,
             max_threads = 1, max_insert_threads = 1, max_block_size = 1000, preferred_block_size_bytes = 0
" &
pid=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT async_insert_pause_before_schedule PAUSE"
${CLICKHOUSE_CLIENT} --lock_acquire_timeout=10 --database_replicated_allow_heavy_create=1 -q "
    CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM dst
"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT async_insert_pause_before_schedule"
wait "$pid"

# The flush ran after `POPULATE`, so it fed the view.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM dst; SELECT count() FROM mv"
