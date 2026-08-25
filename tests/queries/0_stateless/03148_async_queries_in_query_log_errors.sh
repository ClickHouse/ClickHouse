#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function print_flush_query_logs()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log, query_log"
    echo ""

    echo "system.asynchronous_insert_log"
        ${CLICKHOUSE_CLIENT} -q "
          SELECT
              database,
              table,
              query,
              format,
              extract(replace(exception::String, 'Exception', 'Exc*****on'), '.*UInt32:') as error,
              not empty(flush_query_id) as populated_flush_query_id
          FROM system.asynchronous_insert_log
          WHERE
              event_date >= yesterday() AND event_time >= now() - 600
          AND query_id = '$1'
          AND database = currentDatabase()
          FORMAT Vertical"

    echo "system.query_log"
    ${CLICKHOUSE_CLIENT} -q "
      SELECT
          replace(type::String, 'Exception', 'Exc*****on') as type,
          read_rows,
          read_bytes,
          written_rows,
          written_bytes,
          result_rows,
          result_bytes,
          query,
          query_kind,
          databases,
          tables,
          columns,
          views,
          exception_code
      FROM system.query_log
      WHERE
          event_date >= yesterday() AND event_time >= now() - 600
      AND initial_query_id = (SELECT flush_query_id FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND query_id = '$1')
      -- AND current_database = currentDatabase() -- Just to silence style check: this is not ok for this test since the query uses default values
      ORDER BY type DESC
      FORMAT Vertical"
}


${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_landing (id UInt32) ENGINE = MergeTree ORDER BY id"

query_id="$(random_str 10)"
# Both flags are load-bearing and close different drain paths, so keep both: with the adaptive
# timeout on, `pushDataChunk` can auto-schedule the batch immediately, and with it off the deadline
# thread drains the batch once `async_insert_busy_timeout_max_ms` expires (200 ms by default, 5000 ms
# under `tests/config/users.d/timeouts.xml` via the `async_insert_busy_timeout_ms` alias) - either way
# before `SYSTEM FLUSH ASYNC INSERT QUEUE` below, which waits only for the jobs it scheduled itself.
# Client flags, not `SETTINGS`: the reference asserts the logged statement verbatim.
${CLICKHOUSE_CLIENT} --async_insert_use_adaptive_busy_timeout=0 --async_insert_busy_timeout_max_ms=300000 \
    --query_id="${query_id}" -q "INSERT INTO async_insert_landing SETTINGS wait_for_async_insert=0, async_insert=1 values ('Invalid')" 2>/dev/null || true
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE async_insert_landing"
print_flush_query_logs ${query_id}
