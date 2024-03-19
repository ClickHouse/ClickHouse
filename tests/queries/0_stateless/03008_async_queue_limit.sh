#!/usr/bin/env bash
# Tags: no-parallel, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


table="t_03008_async_insert"
log_comment="${table}_$RANDOM"

# sanity check that default wasn't changed
${CLICKHOUSE_CLIENT} -q "SELECT toUInt64(value) FROM system.server_settings WHERE name = 'max_pending_async_inserts'"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE $table(s Int32) ENGINE=MergeTree ORDER BY s"

# use async inserts, but don't allow data to be flushed normally - by data_size or busy_timeout.
# in other words, only forceful flush (when number of pending inserts will exceed threshold) should lead to removing entries from queue.
client_opts=(
  --async_insert 1
  --async_insert_busy_timeout_ms 1000000000
  --async_insert_busy_timeout_max_ms 1000000000
  --async_insert_busy_timeout_min_ms 1000000000
  --async_insert_cleanup_timeout_ms 1000000000
  --async_insert_deduplicate 0
  --async_insert_max_data_size 1000000000000
  --async_insert_max_query_number 1000000000000
  --wait_for_async_insert 0
  --log_comment "$log_comment"
)

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"

${CLICKHOUSE_BENCHMARK} "${client_opts[@]}" -q "INSERT INTO $table VALUES (42)" -i 999 -c 10 &> /dev/null

${CLICKHOUSE_CLIENT} -nq "
  SYSTEM FLUSH LOGS;

  SELECT SUM(ProfileEvents['AsyncInsertQueueFlushesOnLimit'])
    FROM system.query_log
   WHERE current_database = currentDatabase() AND event_date >= yesterday() AND log_comment = '$log_comment' AND type = 'QueryFinish';

  # max_pending_async_inserts + max concurrency
  SELECT throwIf(MAX(pending_inserts) > 510)
    FROM (
      SELECT SUM(CurrentMetric_PendingAsyncInsert) AS pending_inserts
        FROM system.metric_log
       WHERE event_date >= yesterday()
    GROUP BY event_time
    );
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE $table"
