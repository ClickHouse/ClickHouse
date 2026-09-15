#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_flush_event (a UInt64) ENGINE = MergeTree ORDER BY a"

# Do not wait for the flush, and keep the queue from flushing on its own: the data stays
# below the size limit and the busy timeout never elapses. So the explicit SYSTEM FLUSH
# below is the only flush of this table.
for _ in {1..3}; do
    ${CLICKHOUSE_CLIENT} \
        --async_insert 1 \
        --wait_for_async_insert 0 \
        --async_insert_use_adaptive_busy_timeout 0 \
        --async_insert_busy_timeout_min_ms 600000 \
        --async_insert_busy_timeout_max_ms 600000 \
        --async_insert_max_data_size 1000000000 \
        -q "INSERT INTO async_insert_flush_event VALUES (1)"
done

flush_query_id="05218_flush_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "${flush_query_id}" -q "SYSTEM FLUSH ASYNC INSERT QUEUE async_insert_flush_event"

# The three queries share one queue entry, so the flush inserts them as a single batch.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM async_insert_flush_event"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# Read the event from system.query_log instead of system.events. The queue schedules the
# batch on the thread of the SYSTEM FLUSH query, so the event belongs to that query alone
# and tests running in parallel cannot change the number.
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ProfileEvents['AsyncInsertFlush'])
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query_id = '${flush_query_id}'
      AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_insert_flush_event"
