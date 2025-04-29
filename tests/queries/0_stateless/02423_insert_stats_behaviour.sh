#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE floats (v Float64) Engine=MergeTree() ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE target_1 (v Float64) Engine=MergeTree() ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE target_2 (v Float64) Engine=MergeTree() ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW floats_to_target TO target_1 AS SELECT * FROM floats"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW floats_to_target_2 TO target_2 AS SELECT * FROM floats, numbers(2) n"

# Insertions into table without MVs
$CLICKHOUSE_CLIENT -q "INSERT into target_1 FORMAT CSV 1.0"
$CLICKHOUSE_LOCAL -q "SELECT number::Float64 AS v FROM numbers(10)" --format Native | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+target_1+FORMAT+Native" --data-binary @-
$CLICKHOUSE_LOCAL -q "SELECT number::Float64 AS v FROM numbers(10)" --format RowBinary | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+target_1+FORMAT+RowBinary" --data-binary @-

# Insertions into table without 2 MVs (1:1 and 1:2 rows)
$CLICKHOUSE_CLIENT -q "INSERT into floats FORMAT CSV 1.0"
$CLICKHOUSE_LOCAL -q "SELECT number::Float64 AS v FROM numbers(10)" --format Native | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+floats+FORMAT+Native" --data-binary @-
$CLICKHOUSE_LOCAL -q "SELECT number::Float64 AS v FROM numbers(10)" --format RowBinary | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+floats+FORMAT+RowBinary" --data-binary @-

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q \
  "SELECT
    read_rows,
    read_bytes,
    written_rows,
    written_bytes,
    query
  FROM system.query_log
  WHERE
        event_date >= yesterday()
    AND event_time > now() - INTERVAL 600 SECOND
    AND type = 'QueryFinish'
    AND query_kind = 'Insert'
    AND current_database == currentDatabase()
  ORDER BY event_time_microseconds ASC
  FORMAT TSKV"

$CLICKHOUSE_CLIENT -q \
  "SELECT
    read_rows,
    read_bytes,
    written_rows,
    written_bytes,
    ql.query as source_query,
    view_query
  FROM system.query_views_log
  INNER JOIN
  (
      SELECT
        query_id, query, event_time_microseconds
      FROM system.query_log
      WHERE
            event_date >= yesterday()
        AND event_time > now() - INTERVAL 600 SECOND
        AND type = 'QueryFinish'
        AND query_kind = 'Insert'
        AND current_database == currentDatabase()
  ) ql
  ON system.query_views_log.initial_query_id = ql.query_id
  ORDER BY ql.event_time_microseconds ASC, view_query ASC
  FORMAT TSKV"
