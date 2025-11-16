#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function print_flush_query_logs()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
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
              event_date >= yesterday()
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
          event_date >= yesterday()
      AND initial_query_id = (SELECT flush_query_id FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND query_id = '$1')
      -- AND current_database = currentDatabase() -- Just to silence style check: this is not ok for this test since the query uses default values
      ORDER BY type DESC
      FORMAT Vertical"
}


${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_landing (id UInt32) ENGINE = MergeTree ORDER BY id"

query_id="$(random_str 10)"
${CLICKHOUSE_CLIENT} --query_id="${query_id}" -q "INSERT INTO async_insert_landing SETTINGS wait_for_async_insert=0, async_insert=1 values ('Invalid')" 2>/dev/null || true
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
print_flush_query_logs ${query_id}
