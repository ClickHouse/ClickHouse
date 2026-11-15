#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_flush_event (a UInt64) ENGINE = MergeTree ORDER BY a"

# Do not wait for the flush, and keep the queue from flushing on its own: the data stays
# below the size limit and the busy timeout never elapses. So the explicit SYSTEM FLUSH
# below is the only flush of this table.
insert_query_id="05218_insert_${CLICKHOUSE_DATABASE}"
for i in 1 2 3; do
    ${CLICKHOUSE_CLIENT} \
        --query_id "${insert_query_id}_${i}" \
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

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log, query_log"

# The flush runs as a query of its own, and the event belongs to that query, next to the
# rest of the flush accounting. Its own 'current_database' is a default value, so the row
# is found through the flush query id that the asynchronous insert log records. The log is
# ordered by 'database, table, event_date, event_time', so the subquery names the database
# and the table to read only this test's part of it, and both lookups are bounded in time
# to the current run.
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ProfileEvents['AsyncInsertFlush'])
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND type = 'QueryFinish'
      AND query_kind = 'AsyncInsertFlush'
      AND initial_query_id = (
          SELECT flush_query_id FROM system.asynchronous_insert_log
          WHERE database = currentDatabase()
            AND table = 'async_insert_flush_event'
            AND event_date >= yesterday()
            AND query_id = '${insert_query_id}_1')"

# A flush started by SYSTEM FLUSH ASYNC INSERT QUEUE runs in a thread group whose parent is
# that query, so the event reaches it as well. Reading it per query id keeps tests running
# in parallel out of the result.
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ProfileEvents['AsyncInsertFlush'])
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND query_id = '${flush_query_id}'
      AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_insert_flush_event"
