#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function print_flush_query_logs()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
    echo ""
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

    echo ""
    echo "system.query_views_log"
    ${CLICKHOUSE_CLIENT} -q "
      SELECT
          view_name,
          view_type,
          view_query,
          view_target,
          read_rows,
          read_bytes,
          written_rows,
          written_bytes,
          replace(status::String, 'Exception', 'Exc*****on') as status,
          exception_code
      FROM system.query_views_log
      WHERE
          event_date >= yesterday()
      AND initial_query_id = (SELECT flush_query_id FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND query_id = '$1')
      ORDER BY view_name
      FORMAT Vertical"

    echo ""
    echo "system.part_log"
    ${CLICKHOUSE_CLIENT} -q "
      SELECT
          database,
          table,
          partition_id,
          rows
      FROM system.part_log
      WHERE
          event_date >= yesterday()
      AND query_id = (SELECT flush_query_id FROM system.asynchronous_insert_log WHERE event_date >= yesterday() AND query_id = '$1')
      ORDER BY table
      FORMAT Vertical"
}


${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_landing (id UInt32) ENGINE = MergeTree ORDER BY id"

query_id="$(random_str 10)"
${CLICKHOUSE_CLIENT} --query_id="${query_id}" -q "INSERT INTO async_insert_landing SETTINGS wait_for_async_insert=1, async_insert=1 values (1), (2), (3), (4);"
print_flush_query_logs ${query_id}


${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_target (id UInt32) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW async_insert_mv TO async_insert_target AS SELECT id + throwIf(id = 42) AS id FROM async_insert_landing"

query_id="$(random_str 10)"
${CLICKHOUSE_CLIENT} --query_id="${query_id}" -q "INSERT INTO async_insert_landing SETTINGS wait_for_async_insert=1, async_insert=1 values (11), (12), (13);"
print_flush_query_logs ${query_id}


query_id="$(random_str 10)"
${CLICKHOUSE_CLIENT} --query_id="${query_id}" -q "INSERT INTO async_insert_landing SETTINGS wait_for_async_insert=1, async_insert=1 values (42), (12), (13)" 2>/dev/null || true
print_flush_query_logs ${query_id}
